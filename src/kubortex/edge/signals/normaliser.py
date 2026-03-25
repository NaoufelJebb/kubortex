"""Map Alertmanager labels/annotations to Kubortex signal schema."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from kubortex.shared.models.incident import Signal, TargetRef
from kubortex.shared.types import Category, Severity

# ---------------------------------------------------------------------------
# Label → Kubortex mapping tables
# ---------------------------------------------------------------------------

_SEVERITY_MAP: dict[str, Severity] = {
    "critical": Severity.CRITICAL,
    "error": Severity.HIGH,
    "high": Severity.HIGH,
    "warning": Severity.WARNING,
    "info": Severity.INFO,
    "none": Severity.INFO,
}

# AIDEV-NOTE: Category inference is heuristic — maps alertname patterns.
# A more robust approach would use labels like "kubortex.io/category".
_CATEGORY_KEYWORDS: dict[str, Category] = {
    "cpu": Category.RESOURCE_SATURATION,
    "memory": Category.RESOURCE_SATURATION,
    "oom": Category.RESOURCE_SATURATION,
    "disk": Category.RESOURCE_SATURATION,
    "error": Category.ERROR_RATE,
    "5xx": Category.ERROR_RATE,
    "latency": Category.LATENCY,
    "response_time": Category.LATENCY,
    "down": Category.AVAILABILITY,
    "unavailable": Category.AVAILABILITY,
    "deploy": Category.DEPLOYMENT,
    "rollout": Category.DEPLOYMENT,
    "security": Category.SECURITY,
    "capacity": Category.CAPACITY,
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def normalise_severity(raw: str) -> Severity:
    """Map an Alertmanager severity label to a Kubortex Severity enum."""
    return _SEVERITY_MAP.get(raw.lower(), Severity.WARNING)


def infer_category(alertname: str, labels: dict[str, str]) -> Category:
    """Infer a Kubortex Category from alert name and labels."""
    explicit = labels.get("kubortex_category") or labels.get("category")
    if explicit:
        try:
            return Category(explicit)
        except ValueError:
            pass

    lower = alertname.lower()
    for keyword, cat in _CATEGORY_KEYWORDS.items():
        if keyword in lower:
            return cat
    return Category.CUSTOM


def extract_target_ref(labels: dict[str, str]) -> TargetRef | None:
    """Extract a TargetRef from Alertmanager labels when possible."""
    namespace = labels.get("namespace", "")
    # Try common label patterns for Kubernetes workloads
    for kind_label, kind_value in [
        ("deployment", "Deployment"),
        ("statefulset", "StatefulSet"),
        ("daemonset", "DaemonSet"),
    ]:
        name = labels.get(kind_label, "")
        if name and namespace:
            return TargetRef(kind=kind_value, namespace=namespace, name=name)

    # Fallback: use pod label to infer deployment
    pod = labels.get("pod", "")
    if pod and namespace:
        return TargetRef(kind="Pod", namespace=namespace, name=pod)
    return None


def normalise_alert(alert: dict[str, Any]) -> tuple[Signal, Category, TargetRef | None]:
    """Normalise a single Alertmanager alert into Kubortex domain objects.

    Returns (Signal, inferred Category, optional TargetRef).
    """
    labels = alert.get("labels", {})
    annotations = alert.get("annotations", {})

    alertname = labels.get("alertname", "UnknownAlert")
    severity = normalise_severity(labels.get("severity", "warning"))
    category = infer_category(alertname, labels)
    target = extract_target_ref(labels)

    summary = annotations.get("summary") or annotations.get("description") or alertname

    starts_at = alert.get("startsAt")
    observed_at = (
        datetime.fromisoformat(starts_at.replace("Z", "+00:00")) if starts_at else datetime.now(UTC)
    )

    # Collect any numeric values as payload
    payload: dict[str, str] = {}
    value = annotations.get("value") or labels.get("value")
    if value:
        payload["value"] = str(value)

    signal = Signal(
        alertname=alertname,
        severity=severity,
        summary=summary,
        observedAt=observed_at,
        payload=payload,
    )
    return signal, category, target
