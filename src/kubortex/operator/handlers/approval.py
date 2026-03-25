"""Kopf handler for ApprovalRequest CRD lifecycle transitions.

Observes human decisions (via kubectl patch) and timeout expiry.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import kopf
import structlog

from kubortex.shared.k8s import patch_status
from kubortex.shared.types import ApprovalRequestPhase, DecisionType, IncidentPhase

logger = structlog.get_logger(__name__)

GROUP = "kubortex.io"
VERSION = "v1alpha1"
APPROVAL_REQUESTS = "approvalrequests"
INCIDENTS = "incidents"
ACTION_EXECUTIONS = "actionexecutions"


@kopf.on.field(GROUP, VERSION, APPROVAL_REQUESTS, field="status.decision")
async def on_approval_decision(
    body: dict[str, Any],
    name: str,
    namespace: str,
    new: str | None,
    old: str | None,
    **_: Any,
) -> None:
    """Handle human approval or rejection via kubectl patch."""
    if not new or old:
        return

    spec = body.get("spec", {})
    incident_ref = spec.get("incidentRef", "")

    if new == DecisionType.APPROVED:
        await patch_status(
            APPROVAL_REQUESTS,
            name,
            {"phase": ApprovalRequestPhase.APPROVED},
            namespace=namespace,
        )
        logger.info("approval_approved", name=name)

        # Create ActionExecution for the approved action
        action = spec.get("action", {})
        ae_name = f"ae-{incident_ref}-{action.get('id', 'unknown')}"
        from kubortex.shared.k8s import create_resource

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
        await create_resource(ACTION_EXECUTIONS, ae_body, namespace=namespace)
        await patch_status(
            INCIDENTS,
            incident_ref,
            {"phase": IncidentPhase.EXECUTING},
            namespace=namespace,
        )

    elif new == DecisionType.REJECTED:
        await patch_status(
            APPROVAL_REQUESTS,
            name,
            {"phase": ApprovalRequestPhase.REJECTED},
            namespace=namespace,
        )
        await patch_status(
            INCIDENTS,
            incident_ref,
            {"phase": IncidentPhase.ESCALATED},
            namespace=namespace,
        )
        logger.info("approval_rejected", name=name)


@kopf.timer(GROUP, VERSION, APPROVAL_REQUESTS, interval=30)
async def check_approval_timeout(
    body: dict[str, Any],
    name: str,
    namespace: str,
    **_: Any,
) -> None:
    """Time out pending approvals that exceed their deadline."""
    status = body.get("status", {})
    if status.get("phase") != ApprovalRequestPhase.PENDING:
        return

    created = body.get("metadata", {}).get("creationTimestamp")
    timeout_minutes = body.get("spec", {}).get("timeoutMinutes", 30)
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
