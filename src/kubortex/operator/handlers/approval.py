"""Kopf handlers governing ApprovalRequest CRD lifecycle transitions.

Observes human decisions (via kubectl patch) and timeout expiry.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import kopf
import structlog
from kubernetes_asyncio.client import ApiException

from kubortex.operator.settings import GROUP, VERSION, settings
from kubortex.shared.constants import ACTION_EXECUTIONS, APPROVAL_REQUESTS, INCIDENTS
from kubortex.shared.k8s import create_resource, get_resource, patch_status
from kubortex.shared.types import (
    ActionExecutionPhase,
    ApprovalRequestPhase,
    DecisionType,
    IncidentPhase,
)

from ..budget import increment_usage, update_usage

logger = structlog.get_logger(__name__)


@kopf.on.field(GROUP, VERSION, APPROVAL_REQUESTS, field="status.decision")
async def on_approval_decision(
    body: dict[str, Any],
    name: str,
    namespace: str,
    new: str | None,
    old: str | None,
    **_: Any,
) -> None:
    """React to a human decision written to ``status.decision``.

    Guards:
    - ``new`` must be non-empty (a decision was written).
    - ``old`` must be None (prevents re-triggering if the field is updated).

    On ``approved``: creates an ActionExecution (phase=Approved), increments
    the action-type budget counter, and transitions the Incident to Executing.

    On ``rejected``: closes the request and transitions the Incident to
    Escalated — the human has explicitly declined to proceed.

    Args:
        body: ApprovalRequest resource body.
        name: ApprovalRequest name.
        namespace: ApprovalRequest namespace.
        new: Decision value written by the human (``approved`` or ``rejected``).
        old: Previous decision value (expected to be None).
    """
    if not new or old:
        return

    spec = body.get("spec", {})
    incident_ref = spec.get("incidentRef", "")

    if new == DecisionType.APPROVED:
        try:
            await patch_status(
                APPROVAL_REQUESTS,
                name,
                {"phase": ApprovalRequestPhase.APPROVED},
                namespace=namespace,
            )
        except ApiException as exc:
            if exc.status == 404:
                logger.warning("approval_request_gone_on_decision", name=name)
                return
            raise
        logger.info("approval_approved", name=name)

        # Create ActionExecution for the approved action (409 = already created on retry)
        action = spec.get("action", {})
        ae_name = f"ae-{incident_ref}-{action.get('id', 'unknown')}"
        ae_body = {
            "apiVersion": f"{GROUP}/{VERSION}",
            "kind": "ActionExecution",
            "metadata": {
                "name": ae_name,
                "namespace": namespace,
                "labels": {
                    "kubortex.io/incident": incident_ref,
                    "kubortex.io/action-type": action.get("type", ""),
                },
            },
            "spec": {
                "incidentRef": incident_ref,
                "remediationPlanRef": spec.get("remediationPlanRef", ""),
                "approvalRequestRef": name,
                "action": {
                    "type": action.get("type", ""),
                    "target": action.get("target", {}),
                    "parameters": action.get("parameters", {}),
                    "riskTier": action.get("riskTier", "low"),
                },
                "approval": {
                    "decidedBy": body.get("status", {}).get("decidedBy", ""),
                    "decidedAt": body.get("status", {}).get("decidedAt"),
                },
                "rollbackOnRegression": True,
            },
        }
        try:
            await create_resource(ACTION_EXECUTIONS, ae_body, namespace=namespace)
            await patch_status(
                ACTION_EXECUTIONS,
                ae_name,
                {"phase": ActionExecutionPhase.APPROVED},
                namespace=namespace,
            )
        except ApiException as exc:
            if exc.status != 409:
                raise
            logger.info("action_execution_already_exists_on_approval", name=ae_name)

        # Atomically increment budget for the approved action
        try:
            incident_resource = await get_resource(INCIDENTS, incident_ref, namespace=namespace)
            profile_name = (incident_resource.get("status") or {}).get("autonomyProfile", "")
            if profile_name:
                action_type = action.get("type", "")
                await update_usage(profile_name, lambda u: increment_usage(action_type, u))
        except ApiException as exc:
            if exc.status != 404:
                raise
            logger.warning("incident_gone_on_budget_increment", incident=incident_ref)

        try:
            await patch_status(
                INCIDENTS,
                incident_ref,
                {"phase": IncidentPhase.EXECUTING},
                namespace=namespace,
            )
        except ApiException as exc:
            if exc.status != 404:
                raise

    elif new == DecisionType.REJECTED:
        try:
            await patch_status(
                APPROVAL_REQUESTS,
                name,
                {"phase": ApprovalRequestPhase.REJECTED},
                namespace=namespace,
            )
        except ApiException as exc:
            if exc.status == 404:
                logger.warning("approval_request_gone_on_rejection", name=name)
                return
            raise
        try:
            await patch_status(
                INCIDENTS,
                incident_ref,
                {"phase": IncidentPhase.ESCALATED},
                namespace=namespace,
            )
        except ApiException as exc:
            if exc.status != 404:
                raise
        logger.info("approval_rejected", name=name)


@kopf.timer(GROUP, VERSION, APPROVAL_REQUESTS, interval=settings.approval_check_interval)
async def check_approval_timeout(
    body: dict[str, Any],
    name: str,
    namespace: str,
    **_: Any,
) -> None:
    """Expire an ApprovalRequest and escalate the Incident when the timeout is reached.

    Runs on a timer (interval=``settings.approval_check_interval``). Only
    acts when the request is still Pending — approved/rejected requests are
    ignored. The timeout window is read from ``spec.timeoutMinutes``, falling
    back to ``settings.approval_timeout_minutes`` when absent.

    On expiry: transitions ApprovalRequest → TimedOut and Incident →
    Escalated. This ensures the incident is never silently abandoned if the
    on-call engineer misses or ignores the request.

    Args:
        body: ApprovalRequest resource body.
        name: ApprovalRequest name.
        namespace: ApprovalRequest namespace.
    """
    status = body.get("status", {})
    if status.get("phase") != ApprovalRequestPhase.PENDING:
        return

    created = body.get("metadata", {}).get("creationTimestamp")
    timeout_minutes = body.get("spec", {}).get("timeoutMinutes", settings.approval_timeout_minutes)
    if not created:
        return

    created_dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
    if (datetime.now(UTC) - created_dt).total_seconds() > timeout_minutes * 60:
        await patch_status(
            APPROVAL_REQUESTS,
            name,
            {"phase": ApprovalRequestPhase.TIMED_OUT},
            namespace=namespace,
        )
        incident_ref = body.get("spec", {}).get("incidentRef", "")
        if incident_ref:
            await patch_status(
                INCIDENTS,
                incident_ref,
                {"phase": IncidentPhase.ESCALATED},
                namespace=namespace,
            )
        logger.warning("approval_timed_out", name=name)
