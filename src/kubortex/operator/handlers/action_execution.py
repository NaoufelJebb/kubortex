"""Kopf handler for ActionExecution CRD lifecycle transitions.

Observes remediator claims, results, and verification outcomes to
transition ActionExecution and parent Incident phases.
"""

from __future__ import annotations

from typing import Any

import kopf
import structlog

from kubortex.shared.k8s import patch_status
from kubortex.shared.types import ActionExecutionPhase, IncidentPhase

logger = structlog.get_logger(__name__)

GROUP = "kubortex.io"
VERSION = "v1alpha1"
ACTION_EXECUTIONS = "actionexecutions"
INCIDENTS = "incidents"


@kopf.on.field(GROUP, VERSION, ACTION_EXECUTIONS, field="status.claimedBy")
async def on_action_claimed(
    body: dict[str, Any],
    name: str,
    namespace: str,
    new: str | None,
    old: str | None,
    **_: Any,
) -> None:
    """Transition Approved → Executing when a remediator claims the resource."""
    if not new or old:
        return
    status = body.get("status", {})
    if status.get("phase") != ActionExecutionPhase.APPROVED:
        return

    await patch_status(
        ACTION_EXECUTIONS,
        name,
        {"phase": ActionExecutionPhase.EXECUTING},
        namespace=namespace,
    )
    logger.info("action_executing", name=name, claimed_by=new)


@kopf.on.field(GROUP, VERSION, ACTION_EXECUTIONS, field="status.result")
async def on_action_result(
    body: dict[str, Any],
    name: str,
    namespace: str,
    new: str | None,
    **_: Any,
) -> None:
    """Transition based on the remediator's result."""
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
        await patch_status(
            ACTION_EXECUTIONS,
            name,
            {"phase": ActionExecutionPhase.FAILED},
            namespace=namespace,
        )
        if incident_ref:
            await _handle_failure(incident_ref, namespace)
        logger.warning("action_failed", name=name, error=status.get("error"))
    elif improved is False and status.get("rollback", {}).get("triggered"):
        await patch_status(
            ACTION_EXECUTIONS,
            name,
            {"phase": ActionExecutionPhase.ROLLED_BACK},
            namespace=namespace,
        )
        if incident_ref:
            await _handle_failure(incident_ref, namespace)
        logger.warning("action_rolled_back", name=name)
    else:
        await patch_status(
            ACTION_EXECUTIONS,
            name,
            {"phase": ActionExecutionPhase.SUCCEEDED},
            namespace=namespace,
        )
        if incident_ref:
            await patch_status(
                INCIDENTS,
                incident_ref,
                {"phase": IncidentPhase.RESOLVED},
                namespace=namespace,
            )
        logger.info("action_succeeded", name=name)


async def _handle_failure(incident_ref: str, namespace: str) -> None:
    """Handle action failure — check retry budget and escalate or retry."""
    from kubortex.shared.k8s import get_resource

    incident = await get_resource("incidents", incident_ref, namespace=namespace)
    status = incident.get("status", {})
    retry_count = status.get("retryCount", 0)
    max_retries = status.get("maxRetries", 2)

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
