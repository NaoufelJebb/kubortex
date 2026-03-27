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

from kubortex.shared.constants import INCIDENTS
from kubortex.shared.k8s import (
    create_resource,
    get_resource,
    list_resources,
    patch_spec,
    patch_status,
)
from kubortex.shared.models.incident import Signal, TargetRef
from kubortex.shared.types import Category, Severity

logger = structlog.get_logger(__name__)


def _correlation_key(category: Category, target: TargetRef | None) -> str:
    """Build the correlation key for a signal group.

    Args:
        category: Incident category.
        target: Optional target reference.

    Returns:
        Stable correlation key.
    """
    ns = target.namespace if target else ""
    name = target.name if target else ""
    return f"{category}:{ns}/{name}"


def _incident_name(key: str) -> str:
    """Generate a unique incident name from a correlation key.

    Args:
        key: Correlation key.

    Returns:
        Incident name.
    """
    digest = hashlib.sha256(key.encode()).hexdigest()[:8]
    ts = datetime.now(UTC).strftime("%Y%m%d%H%M%S%f")
    return f"inc-{ts}-{digest}"


def _highest_severity(signals: list[Signal]) -> Severity:
    """Return the highest severity in a signal list.

    Args:
        signals: Signals to compare.

    Returns:
        Highest severity present.
    """
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
    crd_group: str = "kubortex.io",
    crd_version: str = "v1alpha1",
    correlation_window_seconds: int = 300,
) -> str:
    """Correlate signals into an incident and upsert the resource.

    Args:
        signals: Signals to correlate.
        category: Incident category.
        target: Optional target reference.
        namespace: Namespace for the incident resource.
        crd_group: Incident CRD API group.
        crd_version: Incident CRD API version.
        correlation_window_seconds: Window for reusing active incidents.

    Returns:
        Name of the existing or created incident.
    """
    key = _correlation_key(category, target)

    existing = await _find_active_incident(category, target, namespace, correlation_window_seconds)
    if existing:
        inc_name = existing["metadata"]["name"]
        await _append_signals(inc_name, signals, namespace)
        logger.info("incident_updated", name=inc_name, new_signals=len(signals))
        return inc_name

    inc_name = _incident_name(key)
    severity = _highest_severity(signals)
    summary = signals[0].summary if signals else "Unknown incident"

    body: dict[str, Any] = {
        "apiVersion": f"{crd_group}/{crd_version}",
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
    correlation_window_seconds: int,
) -> dict[str, Any] | None:
    """Find a matching active incident within the correlation window.

    Args:
        category: Incident category.
        target: Optional target reference.
        namespace: Namespace to search.
        correlation_window_seconds: Maximum incident age to reuse.

    Returns:
        Matching incident resource, or ``None`` when absent.
    """
    incidents = await list_resources(INCIDENTS, namespace=namespace)
    terminal = {"Resolved", "Escalated", "Suppressed"}
    cutoff = datetime.now(UTC) - timedelta(seconds=correlation_window_seconds)
    expected_target = target.model_dump() if target else None

    for inc in incidents:
        phase = (inc.get("status") or {}).get("phase", "Detected")
        if phase in terminal:
            continue
        labels = inc.get("metadata", {}).get("labels", {})
        if labels.get("kubortex.io/category") != category:
            continue
        spec = inc.get("spec") or {}
        actual_target = spec.get("targetRef")
        # NOTE: Keep correlation target-aware to avoid merging unrelated workloads.
        if actual_target != expected_target:
            continue
        created = inc.get("metadata", {}).get("creationTimestamp", "")
        if created:
            created_dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
            if created_dt < cutoff:
                continue
        return inc
    return None


async def _append_signals(inc_name: str, signals: list[Signal], namespace: str) -> None:
    """Append signals to an existing incident.

    Args:
        inc_name: Incident name.
        signals: Signals to append.
        namespace: Incident namespace.
    """
    inc = await get_resource(INCIDENTS, inc_name, namespace=namespace)
    existing_signals = inc.get("spec", {}).get("signals", [])
    new_entries = [s.model_dump(by_alias=True, mode="json") for s in signals]
    merged = existing_signals + new_entries

    await patch_spec(INCIDENTS, inc_name, {"signals": merged}, namespace=namespace)
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
