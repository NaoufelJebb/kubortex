"""Kopf handler for Investigation CRD lifecycle transitions.

Observes claimedBy, result, and timeout to transition Investigation phases.
The operator never performs investigation — only governs phase transitions.
"""

from __future__ import annotations

from typing import Any

import kopf
import structlog
from kubernetes_asyncio.client import ApiException

from kubortex.operator.settings import GROUP, VERSION
from kubortex.shared.constants import INCIDENTS, INVESTIGATIONS
from kubortex.shared.k8s import patch_status
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
            namespace=namespace,
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
    """Transition Investigation → Completed and advance the parent Incident.

    Triggered when the investigator worker writes ``status.result``. The
    handler:
    1. Transitions the Investigation to Completed.
    2. Copies a synopsis (hypothesis, confidence, evidence/action counts)
       into ``Incident.status.investigation``.
    3. If ``result.escalate=True``, transitions Incident → Escalated
       (investigator determined autonomous remediation is inappropriate).
       Otherwise, transitions Incident → RemediationPlanned to signal the
       remediator worker that a plan is ready to evaluate.

    Guard: phase must be InProgress to prevent duplicate processing.

    Args:
        body: Investigation resource body.
        name: Investigation name.
        namespace: Investigation namespace.
        new: Result payload written by the investigator worker.
    """
    if not isinstance(new, dict) or not new:
        return
    status = body.get("status", {})
    if status.get("phase") != InvestigationPhase.IN_PROGRESS:
        return

    try:
        await patch_status(
            INVESTIGATIONS,
            name,
            {"phase": InvestigationPhase.COMPLETED},
            namespace=namespace,
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
            namespace=namespace,
        )
        if new.get("escalate"):
            await patch_status(
                INCIDENTS,
                incident_ref,
                {"phase": IncidentPhase.ESCALATED},
                namespace=namespace,
            )
            logger.warning("investigation_result_escalate", name=name, incident=incident_ref)
        else:
            await patch_status(
                INCIDENTS,
                incident_ref,
                {"phase": IncidentPhase.REMEDIATION_PLANNED},
                namespace=namespace,
            )
            logger.info("incident_remediation_planned", name=name, incident=incident_ref)
    except ApiException as exc:
        if exc.status == 404:
            logger.warning("incident_gone_on_investigation_result", name=name, incident=incident_ref)
        else:
            raise


@kopf.on.field(GROUP, VERSION, INVESTIGATIONS, field="status.phase")
async def on_investigation_phase_terminal(
    body: dict[str, Any],
    name: str,
    namespace: str,
    new: str | None,
    **_: Any,
) -> None:
    """Transition parent Incident → Escalated on abnormal Investigation termination.

    Fires when ``status.phase`` changes to TimedOut or Cancelled — both
    indicate the investigator worker did not produce a result (timeout
    exceeded, or the worker explicitly abandoned the investigation). In
    either case the incident cannot progress autonomously and must be
    handed to a human.

    All other phase values are ignored; the normal completion path is
    handled by ``on_investigation_result``.

    Args:
        body: Investigation resource body.
        name: Investigation name.
        namespace: Investigation namespace.
        new: New phase value (only acts on TimedOut or Cancelled).
    """
    if new not in (InvestigationPhase.TIMED_OUT, InvestigationPhase.CANCELLED):
        return
    incident_ref = body.get("spec", {}).get("incidentRef", "")
    if not incident_ref:
        return
    try:
        await patch_status(
            INCIDENTS,
            incident_ref,
            {"phase": IncidentPhase.ESCALATED},
            namespace=namespace,
        )
    except ApiException as exc:
        if exc.status == 404:
            logger.warning("incident_gone_on_investigation_terminal", name=name, incident=incident_ref)
            return
        raise
    logger.warning("investigation_terminal_escalates", name=name, phase=new, incident=incident_ref)
