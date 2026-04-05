"""Kopf handler for Investigation CRD lifecycle transitions.

Observes claimedBy and result to transition Investigation phases.
The operator never performs investigation — only governs phase transitions.
"""

from __future__ import annotations

from typing import Any

import kopf
import structlog
from kubernetes_asyncio.client import ApiException

from kubortex.operator.settings import GROUP, VERSION
from kubortex.shared.constants import INCIDENTS, INVESTIGATIONS, REMEDIATION_PLANS
from kubortex.shared.crds import create_resource, patch_status
from kubortex.shared.types import IncidentPhase, InvestigationPhase

logger = structlog.get_logger(__name__)


@kopf.on.field(GROUP, VERSION, INVESTIGATIONS, field="status.claimedBy")
async def on_investigation_claimed(
    body: dict[str, Any],
    name: str,
    namespace: str,
    new: str | None,
    old: str | None,
    **_: Any,
) -> None:
    """Transition Investigation -> InProgress when an investigator worker claims it.

    Guards:
    - ``new`` must be non-empty (a worker ID was written).
    - ``old`` must be None (prevents re-triggering on identity updates).
    - Current phase must be Pending (idempotency — ignores stale events).

    Args:
        body: Investigation resource body.
        name: Investigation name.
        namespace: Investigation namespace.
        new: Worker ID that claimed the investigation.
        old: Previous claimant value (expected to be None).
    """
    if not new or old:
        return
    status = body.get("status", {})
    if status.get("phase") != InvestigationPhase.PENDING:
        return

    try:
        await patch_status(
            INVESTIGATIONS,
            name,
            {"phase": InvestigationPhase.IN_PROGRESS},
        )
    except ApiException as exc:
        if exc.status == 404:
            logger.warning("investigation_gone_on_claim", name=name)
            return
        raise
    logger.info("investigation_in_progress", name=name, claimed_by=new)


@kopf.on.field(GROUP, VERSION, INVESTIGATIONS, field="status.result")
async def on_investigation_result(
    body: dict[str, Any],
    name: str,
    namespace: str,
    new: Any | None,
    **_: Any,
) -> None:
    """Transition Investigation → Completed, advance the parent Incident, and create a RemediationPlan.

    Triggered when the investigator worker writes ``status.result``. The
    handler:
    1. Transitions the Investigation to Completed.
    2. Copies a synopsis (hypothesis, confidence, evidence/action counts)
       into ``Incident.status.investigation``.
    3. If ``result.escalate=True``, transitions Incident → Escalated.
       Otherwise, transitions Incident → RemediationPlanned and creates
       a RemediationPlan CR from the investigation's recommendedActions.

    Guard: idempotency check — exits early if already Completed.

    Args:
        body: Investigation resource body.
        name: Investigation name.
        namespace: Investigation namespace.
        new: Result payload written by the investigator worker.
    """
    if not isinstance(new, dict) or not new:
        return
    status = body.get("status", {})
    if status.get("phase") == InvestigationPhase.COMPLETED:
        return

    try:
        await patch_status(
            INVESTIGATIONS,
            name,
            {"phase": InvestigationPhase.COMPLETED},
        )
    except ApiException as exc:
        if exc.status == 404:
            logger.warning("investigation_gone_on_result", name=name)
            return
        raise
    logger.info("investigation_completed", name=name)

    # Notify the parent Incident with synopsis and next phase
    incident_ref = body.get("spec", {}).get("incidentRef", "")
    if not incident_ref:
        return

    synopsis = {
        "hypothesis": new.get("hypothesis", ""),
        "confidence": new.get("confidence", 0.0),
        "evidenceCount": len(new.get("evidence", [])),
        "proposedActionCount": len(new.get("recommendedActions", [])),
    }
    try:
        await patch_status(
            INCIDENTS,
            incident_ref,
            {"investigation": synopsis},
        )
        if new.get("escalate"):
            await patch_status(
                INCIDENTS,
                incident_ref,
                {"phase": IncidentPhase.ESCALATED},
            )
            logger.warning("investigation_result_escalate", name=name, incident=incident_ref)
        else:
            await patch_status(
                INCIDENTS,
                incident_ref,
                {"phase": IncidentPhase.REMEDIATION_PLANNED},
            )
            logger.info("incident_remediation_planned", name=name, incident=incident_ref)

            # Create the RemediationPlan CR from the investigation result
            rp_name = f"rp-{name}"
            rp_body = {
                "apiVersion": f"{GROUP}/{VERSION}",
                "kind": "RemediationPlan",
                "metadata": {
                    "name": rp_name,
                    "namespace": namespace,
                    "labels": {"kubortex.io/incident": incident_ref},
                },
                "spec": {
                    "incidentRef": incident_ref,
                    "investigationRef": name,
                    "hypothesis": new.get("hypothesis", ""),
                    "confidence": new.get("confidence", 0.0),
                    "actions": [
                        {
                            "id": f"a{i}",
                            "type": a.get("type", ""),
                            "target": a.get("target", {}),
                            "parameters": a.get("parameters", {}),
                            "rationale": a.get("rationale", ""),
                        }
                        for i, a in enumerate(new.get("recommendedActions", []))
                    ],
                },
            }
            try:
                await create_resource(REMEDIATION_PLANS, rp_body)
                logger.info("remediation_plan_created", name=rp_name, incident=incident_ref)
            except ApiException as exc:
                if exc.status == 409:
                    logger.info("remediation_plan_already_exists", name=rp_name)
                elif exc.status != 404:
                    raise

    except ApiException as exc:
        if exc.status == 404:
            logger.warning("incident_gone_on_investigation_result", name=name, incident=incident_ref)
        else:
            raise
