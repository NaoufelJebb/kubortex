"""Kopf handlers governing ActionExecution CRD lifecycle transitions.

Observes remediator claims, results, and verification outcomes to
transition ActionExecution and parent Incident phases.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import kopf
import structlog
from kubernetes_asyncio.client import ApiException

from kubortex.operator.settings import GROUP, VERSION, settings
from kubortex.shared.constants import ACTION_EXECUTIONS, INCIDENTS
from kubortex.shared.k8s import get_resource, patch_status
from kubortex.shared.types import ActionExecutionPhase, IncidentPhase

from ..budget import decrement_active, load_usage, persist_usage

logger = structlog.get_logger(__name__)


@kopf.on.field(GROUP, VERSION, ACTION_EXECUTIONS, field="status.claimedBy")
async def on_action_claimed(
    body: dict[str, Any],
    name: str,
    namespace: str,
    new: str | None,
    old: str | None,
    **_: Any,
) -> None:
    """Transition ActionExecution → Executing when a remediator worker claims it.

    Guards:
    - ``new`` must be non-empty (a worker ID was written).
    - ``old`` must be None (prevents re-triggering on identity updates).
    - Current phase must be Approved (idempotency — ignores stale events).

    Args:
        body: ActionExecution resource body.
        name: ActionExecution name.
        namespace: ActionExecution namespace.
        new: Worker ID that claimed the execution.
        old: Previous claimant value (expected to be None).
    """
    if not new or old:
        return
    status = body.get("status", {})
    if status.get("phase") != ActionExecutionPhase.APPROVED:
        return

    try:
        await patch_status(
            ACTION_EXECUTIONS,
            name,
            {"phase": ActionExecutionPhase.EXECUTING},
            namespace=namespace,
        )
    except ApiException as exc:
        if exc.status == 404:
            logger.warning("action_execution_gone_on_claim", name=name)
            return
        raise
    logger.info("action_executing", name=name, claimed_by=new)


@kopf.on.field(GROUP, VERSION, ACTION_EXECUTIONS, field="status.result")
async def on_action_result(
    body: dict[str, Any],
    name: str,
    namespace: str,
    new: Any | None,
    **_: Any,
) -> None:
    """Resolve ActionExecution and advance the parent Incident based on execution outcome.

    Triggered when the remediator worker writes ``status.result``. Evaluates
    three mutually exclusive outcomes in priority order:

    1. ``status.error`` present → Failed; delegates to ``_handle_failure``.
    2. ``status.verification.improved=False`` and rollback was triggered
       → RolledBack; delegates to ``_handle_failure``.
    3. Otherwise → Succeeded; stamps ``resolvedAt``, decrements the active
       remediation budget, and transitions Incident → Resolved.

    Guard: phase must be Executing to prevent duplicate processing.

    Args:
        body: ActionExecution resource body.
        name: ActionExecution name.
        namespace: ActionExecution namespace.
        new: Result value written by the remediator worker.
    """
    if not new:
        return
    status = body.get("status", {})
    if status.get("phase") != ActionExecutionPhase.EXECUTING:
        return

    incident_ref = body.get("spec", {}).get("incidentRef", "")

    # Check verification outcome
    verification = status.get("verification", {})
    improved = verification.get("improved")

    if status.get("error"):
        try:
            await patch_status(
                ACTION_EXECUTIONS,
                name,
                {"phase": ActionExecutionPhase.FAILED},
                namespace=namespace,
            )
        except ApiException as exc:
            if exc.status != 404:
                raise
        if incident_ref:
            await _handle_failure(incident_ref, namespace)
        logger.warning("action_failed", name=name, error=status.get("error"))
    elif improved is False and status.get("rollback", {}).get("triggered"):
        try:
            await patch_status(
                ACTION_EXECUTIONS,
                name,
                {"phase": ActionExecutionPhase.ROLLED_BACK},
                namespace=namespace,
            )
        except ApiException as exc:
            if exc.status != 404:
                raise
        if incident_ref:
            await _handle_failure(incident_ref, namespace)
        logger.warning("action_rolled_back", name=name)
    else:
        try:
            await patch_status(
                ACTION_EXECUTIONS,
                name,
                {"phase": ActionExecutionPhase.SUCCEEDED, "resolvedAt": datetime.now(UTC).isoformat()},
                namespace=namespace,
            )
        except ApiException as exc:
            if exc.status != 404:
                raise
        if incident_ref:
            try:
                incident = await get_resource(INCIDENTS, incident_ref, namespace=namespace)
                profile_name = (incident.get("status") or {}).get("autonomyProfile", "")
                if profile_name:
                    usage = await load_usage(profile_name)
                    usage = decrement_active(usage)
                    await persist_usage(profile_name, usage)
                await patch_status(
                    INCIDENTS,
                    incident_ref,
                    {"phase": IncidentPhase.RESOLVED},
                    namespace=namespace,
                )
            except ApiException as exc:
                if exc.status != 404:
                    raise
                logger.warning("incident_gone_on_action_success", name=name, incident=incident_ref)
        logger.info("action_succeeded", name=name)


async def _handle_failure(incident_ref: str, namespace: str) -> None:
    """Decrement the active budget and retry or escalate the Incident after action failure.

    Called on both Failed and RolledBack outcomes. Reads the incident's
    ``retryCount`` and ``maxRetries`` to decide the next step:
    - ``retryCount < maxRetries`` → Incident → Failed with incremented counter,
      allowing the investigation/remediation cycle to restart.
    - ``retryCount >= maxRetries`` → Incident → Escalated; no further attempts.

    The active remediation budget is decremented before either transition so
    the slot is released regardless of whether a retry will follow.

    Args:
        incident_ref: Name of the parent Incident.
        namespace: Namespace of the parent Incident.
    """
    try:
        incident = await get_resource("incidents", incident_ref, namespace=namespace)
    except ApiException as exc:
        if exc.status == 404:
            logger.warning("incident_gone_on_action_failure", incident=incident_ref)
            return
        raise

    status = incident.get("status", {})
    profile_name = status.get("autonomyProfile", "")
    if profile_name:
        try:
            usage = await load_usage(profile_name)
            usage = decrement_active(usage)
            await persist_usage(profile_name, usage)
        except ApiException as exc:
            if exc.status != 404:
                raise
            logger.warning("profile_gone_on_budget_decrement", profile=profile_name)

    retry_count = status.get("retryCount", 0)
    max_retries = status.get("maxRetries", settings.max_retries)

    if retry_count < max_retries:
        await patch_status(
            INCIDENTS,
            incident_ref,
            {
                "phase": IncidentPhase.FAILED,
                "retryCount": retry_count + 1,
            },
            namespace=namespace,
        )
        logger.info("incident_retry", name=incident_ref, attempt=retry_count + 1)
    else:
        await patch_status(
            INCIDENTS,
            incident_ref,
            {"phase": IncidentPhase.ESCALATED},
            namespace=namespace,
        )
        logger.warning("incident_escalated_max_retries", name=incident_ref)
