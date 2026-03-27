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

# NOTE: Category inference is heuristic and based on alert name patterns.
# Prefer explicit labels such as "kubortex.io/category" when available.
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
    """Map an Alertmanager severity label to a Kubortex enum.

    Args:
        raw: Raw severity label.

    Returns:
        Normalized Kubortex severity.
    """
    return _SEVERITY_MAP.get(raw.lower(), Severity.WARNING)


def infer_category(alertname: str, labels: dict[str, str]) -> Category:
    """Infer a Kubortex category from alert metadata.

    Args:
        alertname: Alert name to inspect.
        labels: Alert labels.

    Returns:
        Inferred Kubortex category.
    """
    explicit = labels.get("kubortex_category") or labels.get("category")
    if explicit:
        try:
            return Category(explicit)
        except ValueError:
            pass

    lower = alertname.lower()
    compact = lower.replace("_", "").replace("-", "")
    for keyword, cat in _CATEGORY_KEYWORDS.items():
        normalised_keyword = keyword.replace("_", "").replace("-", "")
        if keyword in lower or normalised_keyword in compact:
            return cat
    return Category.CUSTOM


def extract_target_ref(labels: dict[str, str]) -> TargetRef | None:
    """Extract a target reference from alert labels.

    Args:
        labels: Alert labels.

    Returns:
        Target reference when one can be inferred, else ``None``.
    """
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
    """Normalize one Alertmanager alert into Kubortex objects.

    Args:
        alert: Raw Alertmanager alert payload.

    Returns:
        Parsed ``(signal, category, target)`` tuple.
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
