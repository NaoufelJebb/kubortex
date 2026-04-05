"""Map Alertmanager labels and annotations to Kubortex signal objects."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import structlog

from kubortex.edge.core.resolver import TargetHints, resolve_target
from kubortex.shared.models.incident import Signal, TargetRef
from kubortex.shared.types import Category, Severity

_SEVERITY_MAP: dict[str, Severity] = {
    "critical": Severity.CRITICAL,
    "error": Severity.HIGH,
    "high": Severity.HIGH,
    "warning": Severity.WARNING,
    "info": Severity.INFO,
    "none": Severity.INFO,
}

logger = structlog.get_logger(__name__)

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


def normalise_severity(raw: str) -> Severity:
    """Map an Alertmanager severity label to a Kubortex enum."""
    return _SEVERITY_MAP.get(raw.lower(), Severity.WARNING)


def infer_category(alertname: str, labels: dict[str, str]) -> Category:
    """Infer a Kubortex category from alert metadata.

    Checks for an explicit ``kubortex.io/category``, ``kubortex_category``, or
    ``category`` label first. Falls back to keyword scanning of the alert name.
    If an explicit label is present but holds an invalid value, a warning is
    logged and keyword inference is attempted instead.

    Args:
        alertname: Alertmanager alert name used for keyword inference.
        labels: Alert label map from the Alertmanager payload.

    Returns:
        Best-matched ``Category`` enum value, or ``Category.CUSTOM`` when no
        keyword matches.
    """
    explicit = (
        labels.get("kubortex.io/category")
        or labels.get("kubortex_category")
        or labels.get("category")
    )
    if explicit:
        try:
            return Category(explicit)
        except ValueError:
            logger.warning(
                "invalid_category_label",
                value=explicit,
                alertname=alertname,
            )

    lower = alertname.lower()
    compact = lower.replace("_", "").replace("-", "")
    for keyword, cat in _CATEGORY_KEYWORDS.items():
        normalized_keyword = keyword.replace("_", "").replace("-", "")
        if keyword in lower or normalized_keyword in compact:
            return cat
    return Category.CUSTOM


def _first_label(labels: dict[str, str], *names: str) -> str:
    for name in names:
        value = labels.get(name, "")
        if value:
            return value
    return ""


def extract_target_hints(labels: dict[str, str]) -> TargetHints:
    """Extract source-agnostic target hints from alert labels."""
    return TargetHints(
        namespace=_first_label(labels, "namespace", "exported_namespace", "kubernetes_namespace"),
        pod=_first_label(labels, "pod", "pod_name", "kubernetes_pod_name"),
        deployment=_first_label(labels, "deployment", "kubernetes_deployment_name"),
        statefulset=_first_label(labels, "statefulset", "kubernetes_statefulset_name"),
        daemonset=_first_label(labels, "daemonset", "kubernetes_daemonset_name"),
        service=_first_label(labels, "service", "kubernetes_service_name"),
        ingress=_first_label(labels, "ingress", "kubernetes_ingress_name"),
        node=_first_label(labels, "node", "kubernetes_node"),
        pvc=_first_label(
            labels,
            "persistentvolumeclaim",
            "pvc",
            "kubernetes_persistentvolumeclaim_name",
        ),
        raw_labels=dict(labels),
    )


async def normalise_alert(alert: dict[str, Any]) -> tuple[Signal, Category, TargetRef | None]:
    """Normalize one Alertmanager alert into Kubortex objects."""
    labels = alert.get("labels", {})
    annotations = alert.get("annotations", {})

    if not isinstance(labels, dict):
        raise ValueError("alert.labels must be a JSON object")
    if not isinstance(annotations, dict):
        raise ValueError("alert.annotations must be a JSON object")

    alertname = str(labels.get("alertname", "UnknownAlert"))
    severity = normalise_severity(str(labels.get("severity", "warning")))
    category = infer_category(alertname, labels)
    target = await resolve_target(extract_target_hints(labels))

    summary = annotations.get("summary") or annotations.get("description") or alertname

    starts_at = alert.get("startsAt")
    if starts_at:
        try:
            observed_at = datetime.fromisoformat(str(starts_at).replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("alert.startsAt must be an ISO 8601 timestamp") from exc
    else:
        observed_at = datetime.now(UTC)

    payload: dict[str, str] = {}
    value = annotations.get("value") or labels.get("value")
    if value is not None:
        payload["value"] = str(value)

    signal = Signal(
        alertname=alertname,
        severity=severity,
        summary=str(summary),
        observedAt=observed_at,
        payload=payload,
    )
    return signal, category, target
