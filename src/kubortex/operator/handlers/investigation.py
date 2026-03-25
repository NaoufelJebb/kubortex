"""Kopf handler for Investigation CRD lifecycle transitions.

Observes claimedBy, result, and timeout to transition Investigation phases.
The operator never performs investigation — only governs phase transitions.
"""

from __future__ import annotations

from typing import Any

import kopf
import structlog

from kubortex.shared.k8s import patch_status
from kubortex.shared.types import InvestigationPhase

logger = structlog.get_logger(__name__)

GROUP = "kubortex.io"
VERSION = "v1alpha1"
INVESTIGATIONS = "investigations"
INCIDENTS = "incidents"


@kopf.on.field(GROUP, VERSION, INVESTIGATIONS, field="status.claimedBy")
async def on_investigation_claimed(
    body: dict[str, Any],
    name: str,
    namespace: str,
    new: str | None,
    old: str | None,
    **_: Any,
) -> None:
    """Transition Pending → InProgress when an investigator claims the resource."""
    if not new or old:
        return
    status = body.get("status", {})
    if status.get("phase") != InvestigationPhase.PENDING:
        return

    await patch_status(
        INVESTIGATIONS,
        name,
        {"phase": InvestigationPhase.IN_PROGRESS},
        namespace=namespace,
    )
    logger.info("investigation_in_progress", name=name, claimed_by=new)


@kopf.on.field(GROUP, VERSION, INVESTIGATIONS, field="status.result")
async def on_investigation_result(
    body: dict[str, Any],
    name: str,
    namespace: str,
    new: Any | None,
    **_: Any,
) -> None:
    """Transition InProgress → Completed when the investigator writes results."""
    if not new:
        return
    status = body.get("status", {})
    if status.get("phase") != InvestigationPhase.IN_PROGRESS:
        return

    await patch_status(
        INVESTIGATIONS,
        name,
        {"phase": InvestigationPhase.COMPLETED},
        namespace=namespace,
    )
    logger.info("investigation_completed", name=name)

    # Notify the parent Incident handler by patching investigation synopsis
    incident_ref = body.get("spec", {}).get("incidentRef", "")
    if incident_ref and isinstance(new, dict):
        synopsis = {
            "hypothesis": new.get("hypothesis", ""),
            "confidence": new.get("confidence", 0.0),
            "evidenceCount": len(new.get("evidence", [])),
            "proposedActionCount": len(new.get("recommendedActions", [])),
        }
        await patch_status(
            INCIDENTS,
            incident_ref,
            {"investigation": synopsis},
            namespace=namespace,
        )
