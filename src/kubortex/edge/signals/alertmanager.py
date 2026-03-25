"""FastAPI router for Alertmanager webhook ingestion.

Receives POST /api/v1/alerts from Alertmanager, normalises each alert,
groups them by correlation key, and creates/updates Incident CRDs.
"""

from __future__ import annotations

from typing import Any

import structlog
from fastapi import APIRouter, Request, Response

from kubortex.shared.config import KubortexSettings
from kubortex.shared.models.incident import Signal, TargetRef
from kubortex.shared.types import Category

from .correlator import correlate_and_upsert
from .normaliser import normalise_alert

logger = structlog.get_logger(__name__)

router = APIRouter(prefix="/api/v1", tags=["signals"])


@router.post("/alerts")
async def receive_alerts(request: Request) -> Response:
    """Ingest an Alertmanager webhook payload.

    Alertmanager sends ``{"alerts": [...]}`` where each alert has labels,
    annotations, startsAt, endsAt, and status.
    """
    body: dict[str, Any] = await request.json()
    alerts = body.get("alerts", [])
    if not alerts:
        return Response(status_code=200, content="no alerts")

    settings = KubortexSettings()
    namespace = settings.namespace

    # Group normalised signals by (category, target)
    groups: dict[str, tuple[list[Signal], Category, TargetRef | None]] = {}

    for alert in alerts:
        # Skip resolved alerts — we only act on firing
        if alert.get("status") == "resolved":
            continue

        signal, category, target = normalise_alert(alert)
        key = f"{category}:{target.namespace}/{target.name}" if target else f"{category}:unknown"
        if key not in groups:
            groups[key] = ([], category, target)
        groups[key][0].append(signal)

    # Upsert each group into an Incident
    created_incidents: list[str] = []
    for _key, (signals, category, target) in groups.items():
        inc_name = await correlate_and_upsert(signals, category, target, namespace)
        created_incidents.append(inc_name)

    logger.info(
        "alerts_ingested",
        alert_count=len(alerts),
        incident_count=len(created_incidents),
    )
    return Response(status_code=200, content=f"processed {len(alerts)} alerts")
