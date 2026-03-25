"""Group and deduplicate signals into Incidents.

Correlation key: (category, target_namespace, target_name).
Signals arriving within the correlation window update the existing Incident
rather than creating a new one.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from typing import Any

import structlog

from kubortex.shared.k8s import create_resource, get_resource, list_resources, patch_status
from kubortex.shared.models.incident import Signal, TargetRef
from kubortex.shared.types import Category, Severity

logger = structlog.get_logger(__name__)

GROUP = "kubortex.io"
VERSION = "v1alpha1"
INCIDENTS = "incidents"

CORRELATION_WINDOW_SECONDS = 300  # 5 minutes


def _correlation_key(category: Category, target: TargetRef | None) -> str:
    """Deterministic correlation key for grouping signals."""
    ns = target.namespace if target else ""
    name = target.name if target else ""
    return f"{category}:{ns}/{name}"


def _incident_name(key: str) -> str:
    """Generate a stable incident name from the correlation key."""
    digest = hashlib.sha256(key.encode()).hexdigest()[:8]
    ts = datetime.now(UTC).strftime("%Y%m%d")
    return f"inc-{ts}-{digest}"


def _highest_severity(signals: list[Signal]) -> Severity:
    """Return the highest severity across signals."""
    order = list(Severity)
    best = 0
    for s in signals:
        idx = order.index(s.severity)
        if idx > best:
            best = idx
    return order[best]


async def correlate_and_upsert(
    signals: list[Signal],
    category: Category,
    target: TargetRef | None,
    namespace: str,
) -> str:
    """Correlate signals to an existing or new Incident, returning the Incident name."""
    key = _correlation_key(category, target)

    # Look for an active Incident with matching labels
    existing = await _find_active_incident(category, target, namespace)
    if existing:
        inc_name = existing["metadata"]["name"]
        await _append_signals(inc_name, signals, namespace)
        logger.info("incident_updated", name=inc_name, new_signals=len(signals))
        return inc_name

    # Create a new Incident
    inc_name = _incident_name(key)
    severity = _highest_severity(signals)
    summary = signals[0].summary if signals else "Unknown incident"

    body: dict[str, Any] = {
        "apiVersion": f"{GROUP}/{VERSION}",
        "kind": "Incident",
        "metadata": {
            "name": inc_name,
            "namespace": namespace,
            "labels": {
                "kubortex.io/severity": severity,
                "kubortex.io/category": category,
            },
        },
        "spec": {
            "severity": severity,
            "category": category,
            "summary": summary,
            "source": "alertmanager",
            "signals": [s.model_dump(by_alias=True, mode="json") for s in signals],
            "targetRef": target.model_dump() if target else None,
        },
    }
    await create_resource(INCIDENTS, body, namespace=namespace)
    logger.info("incident_created", name=inc_name, severity=severity, category=category)
    return inc_name


async def _find_active_incident(
    category: Category,
    target: TargetRef | None,
    namespace: str,
) -> dict[str, Any] | None:
    """Find an active (non-resolved, non-escalated) Incident with matching labels."""
    incidents = await list_resources(INCIDENTS, namespace=namespace)
    terminal = {"Resolved", "Escalated", "Suppressed"}
    cutoff = datetime.now(UTC) - timedelta(seconds=CORRELATION_WINDOW_SECONDS)

    for inc in incidents:
        phase = (inc.get("status") or {}).get("phase", "Detected")
        if phase in terminal:
            continue
        labels = inc.get("metadata", {}).get("labels", {})
        if labels.get("kubortex.io/category") != category:
            continue
        # Check creation time within correlation window
        created = inc.get("metadata", {}).get("creationTimestamp", "")
        if created:
            created_dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
            if created_dt < cutoff:
                continue
        return inc
    return None


async def _append_signals(inc_name: str, signals: list[Signal], namespace: str) -> None:
    """Append new signals to an existing Incident's spec."""
    inc = await get_resource(INCIDENTS, inc_name, namespace=namespace)
    existing_signals = inc.get("spec", {}).get("signals", [])
    new_entries = [s.model_dump(by_alias=True, mode="json") for s in signals]
    existing_signals.extend(new_entries)

    await patch_status(
        INCIDENTS,
        inc_name,
        {
            "timeline": [
                {
                    "timestamp": datetime.now(UTC).isoformat(),
                    "event": "SignalReceived",
                    "detail": f"Added {len(signals)} new signal(s)",
                }
            ],
        },
        namespace=namespace,
    )
