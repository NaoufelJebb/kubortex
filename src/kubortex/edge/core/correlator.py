"""Core signal correlation workflow for Edge.

Signals are correlated by operational identity, using
``(target_kind, target_namespace, target_name)``.
Incidents are resolved by probing deterministic time-bucketed name candidates
derived from that identity.
"""

from __future__ import annotations

import asyncio
import hashlib
import random
from datetime import UTC, datetime, timedelta
from typing import Any

import structlog
from kubernetes_asyncio.client import ApiException

from kubortex.shared.constants import INCIDENTS
from kubortex.shared.crds import (
    create_resource,
    get_resource,
    patch_spec,
    resource_created_at,
)
from kubortex.shared.models.incident import Signal, TargetRef
from kubortex.shared.types import Category, Severity

logger = structlog.get_logger(__name__)

_TERMINAL_PHASES = {"Resolved", "Escalated", "Suppressed", "Failed"}
_MAX_UPDATE_RETRIES = 5
_RETRY_BASE_BACKOFF = 0.05
_RETRY_MAX_BACKOFF = 2.0
_CANDIDATE_BUCKET_OFFSETS = (0, 1)


def _backoff_seconds(attempt: int) -> float:
    """Compute exponential backoff with full jitter for a retry attempt.

    The sleep duration is uniformly sampled from ``[0, min(base * 2^attempt, max)]``
    so that concurrent retrying writers do not all wake at the same instant.

    Args:
        attempt: Zero-based retry attempt index.

    Returns:
        Seconds to sleep before the next attempt.
    """
    ceiling = min(_RETRY_BASE_BACKOFF * (2**attempt), _RETRY_MAX_BACKOFF)
    return random.uniform(0, ceiling)


def _correlation_key(target: TargetRef | None) -> str:
    """Build the correlation key from the target workload identity.

    Args:
        target: Optional target reference.

    Returns:
        Stable correlation key string.
    """
    if target is None:
        return "::/unknown"
    return f"{target.kind}:{target.namespace}/{target.name}"


def _incident_name(
    key: str,
    correlation_window_seconds: int,
    *,
    now: datetime | None = None,
) -> str:
    """Generate the canonical incident name for a correlation key and bucket.

    Args:
        key: Correlation key.
        correlation_window_seconds: Correlation window used to bucket names.
        now: Current timestamp override for deterministic testing.

    Returns:
        Canonical incident name.
    """
    current = now or datetime.now(UTC)
    digest = hashlib.sha256(key.encode()).hexdigest()[:8]
    window_seconds = max(1, correlation_window_seconds)
    bucket = int(current.timestamp()) // window_seconds
    return f"inc-{bucket}-{digest}"


def _candidate_incident_names(
    key: str,
    correlation_window_seconds: int,
    *,
    now: datetime | None = None,
) -> list[str]:
    """Return canonical incident names to probe for the current request.

    The current bucket is probed first. The previous bucket is also probed
    because a still-reusable incident may have been created just before the
    current bucket boundary.

    Args:
        key: Correlation key.
        correlation_window_seconds: Correlation window used to bucket names.
        now: Current timestamp override for deterministic testing.

    Returns:
        Ordered list of canonical incident names to probe.
    """
    current = now or datetime.now(UTC)
    window_seconds = max(1, correlation_window_seconds)
    current_bucket = int(current.timestamp()) // window_seconds
    names: list[str] = []

    for offset in _CANDIDATE_BUCKET_OFFSETS:
        bucket_now = datetime.fromtimestamp(
            (current_bucket - offset) * window_seconds,
            tz=UTC,
        )
        names.append(
            _incident_name(
                key,
                correlation_window_seconds,
                now=bucket_now,
            )
        )

    return names


def _incident_phase(resource: dict[str, Any]) -> str:
    """Return the current Incident phase, defaulting to Detected."""
    return (resource.get("status") or {}).get("phase", "Detected")


def _incident_matches_key(resource: dict[str, Any], key: str) -> bool:
    """Return whether an Incident resource matches a correlation key.

    Matching is based purely on target identity — the same criterion used to
    build *key* via ``_correlation_key``.
    """
    target = (resource.get("spec") or {}).get("targetRef")
    try:
        target_ref = TargetRef.model_validate(
            target) if isinstance(target, dict) else None
    except Exception:
        return False
    return _correlation_key(target_ref) == key


def _incident_is_reusable(
    resource: dict[str, Any],
    key: str,
    correlation_window_seconds: int,
) -> bool:
    """Return whether an Incident can still absorb signals for the key."""
    cutoff = datetime.now(UTC) - timedelta(seconds=correlation_window_seconds)
    if _incident_phase(resource) in _TERMINAL_PHASES:
        return False
    if resource_created_at(resource) < cutoff:
        return False
    return _incident_matches_key(resource, key)


_SEVERITY_ORDER = [s.value for s in Severity]


def _severity_index(raw: str) -> int:
    """Return the ordinal position of a severity string.

    Unknown values map to 0 (lowest) so they never trigger escalation.

    Args:
        raw: Raw severity string (e.g. ``"critical"``).

    Returns:
        Position in ``_SEVERITY_ORDER``, or 0 if unrecognised.
    """
    try:
        return _SEVERITY_ORDER.index(raw)
    except ValueError:
        return 0


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


def _highest_severity_from_raw(signal_dicts: list[dict[str, Any]]) -> str:
    """Return the highest severity value from a list of raw signal dicts.

    Args:
        signal_dicts: Serialised signal objects as stored in ``spec.signals``.

    Returns:
        Highest severity string found, or ``"info"`` when the list is empty.
    """
    best = 0
    for s in signal_dicts:
        idx = _severity_index(s.get("severity", "info"))
        if idx > best:
            best = idx
    return _SEVERITY_ORDER[best] if signal_dicts else Severity.INFO.value


def _dedup_signals(
    existing: list[dict[str, Any]],
    new: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Filter out already observed signals.

    Args:
        existing: Serialised signals already stored in ``spec.signals``.
        new: Incoming serialised signals to filter.

    Returns:
        Subset of *new* that are not duplicates of any existing signal.
    """
    seen: set[tuple[str, str]] = {
        (s["alertname"], s.get("observedAt", "")) for s in existing}
    return [s for s in new if (s["alertname"], s.get("observedAt", "")) not in seen]


async def correlate_and_upsert(
    signals: list[Signal],
    categories: list[Category],
    target: TargetRef | None,
    namespace: str,
    crd_group: str = "kubortex.io",
    crd_version: str = "v1alpha1",
    correlation_window_seconds: int = 300,
    *,
    source: str = "",
    max_signals: int = 200,
) -> str:
    """Correlate signals into an incident and upsert the resource.

    The correlation key is derived from the target workload identity only —
    category is not part of the key. This means signals from different
    categories targeting the same workload are absorbed into one Incident.

    Existing incidents are discovered by probing the canonical incident names
    for the current and previous correlation buckets.

    Matching incidents are updated in a single atomic ``patch_spec`` call with
    deduplication, signal-cap enforcement, and severity escalation.

    Args:
        signals: Signals to correlate.
        categories: One or more observed categories for this signal batch.
        target: Optional target reference (workload identity).
        namespace: Namespace for the incident resource.
        crd_group: Incident CRD API group.
        crd_version: Incident CRD API version.
        correlation_window_seconds: Window for reusing active incidents.
        source: Adapter identifier stamped on the incident (e.g. ``"alertmanager"``).
        max_signals: Maximum number of signals to retain per incident.

    Returns:
        Name of the existing or created incident.
    """
    key = _correlation_key(target)

    existing = await _find_active_incident(target, correlation_window_seconds)
    if existing:
        inc_name = existing["metadata"]["name"]
        await _update_incident(inc_name, signals, categories, max_signals=max_signals)
        logger.info("incident_updated", name=inc_name,
                    new_signals=len(signals))
        return inc_name

    severity = _highest_severity(signals)
    summary = signals[0].summary if signals else "Unknown incident"
    now = datetime.now(UTC)

    inc_name = _incident_name(
        key,
        correlation_window_seconds,
        now=now,
    )
    body: dict[str, Any] = {
        "apiVersion": f"{crd_group}/{crd_version}",
        "kind": "Incident",
        "metadata": {
            "name": inc_name,
            "namespace": namespace,
        },
        "spec": {
            "severity": severity,
            "categories": [c.value for c in categories],
            "summary": summary,
            "source": source,
            "signals": [s.model_dump(by_alias=True, mode="json") for s in signals],
            "targetRef": target.model_dump() if target else None,
        },
    }
    if target is not None:
        body["metadata"]["labels"] = {
            "kubortex.io/target-kind": target.kind,
            "kubortex.io/target-ns": target.namespace,
            "kubortex.io/target-name": target.name,
        }
    try:
        await create_resource(INCIDENTS, body)
        logger.info(
            "incident_created",
            name=inc_name,
            severity=severity,
            categories=[c.value for c in categories],
        )
        return inc_name
    except ApiException as exc:
        if exc.status != 409:
            # Creation errors other than "already exists" are hard failures
            # for this batch and must propagate to the caller unchanged.
            raise

    try:
        conflicting = await get_resource(INCIDENTS, inc_name)
    except ApiException as exc:
        if exc.status == 404:
            raise RuntimeError(
                "canonical incident create conflicted but the resource could not be read"
            ) from exc
        # A non-404 read failure after a create conflict means the canonical
        # incident exists in an unknown state, so do not continue correlation.
        raise

    if _incident_is_reusable(conflicting, key, correlation_window_seconds):
        await _update_incident(inc_name, signals, categories, max_signals=max_signals)
        logger.info("incident_updated", name=inc_name,
                    new_signals=len(signals))
        return inc_name

    raise RuntimeError(
        "canonical incident name is occupied by a non-reusable resource")


async def _find_active_incident(
    target: TargetRef | None,
    correlation_window_seconds: int,
) -> dict[str, Any] | None:
    """Find a matching active incident for a given target.

    Matching incidents are discovered by probing the canonical incident names
    for the current and previous correlation buckets.

    Args:
        target: Optional target reference.
        correlation_window_seconds: Maximum incident age to reuse.

    Returns:
        Matching incident resource, or ``None`` when absent.
    """
    key = _correlation_key(target)

    for inc_name in _candidate_incident_names(key, correlation_window_seconds):
        try:
            incident = await get_resource(INCIDENTS, inc_name)
        except ApiException as exc:
            if exc.status == 404:
                continue
            # Any non-404 API failure means incident discovery could not
            # complete reliably, so abort correlation instead of treating it
            # as a cache miss and probing the next candidate.
            raise

        if _incident_is_reusable(incident, key, correlation_window_seconds):
            return incident

    return None


async def _update_incident(
    inc_name: str,
    signals: list[Signal],
    categories: list[Category],
    *,
    max_signals: int = 200,
) -> None:
    """Update an existing incident with new signals and categories in one atomic spec write.

    Each update pass:
    1. Deduplicates incoming signals against those already stored.
    2. Merges and caps the signal list at *max_signals* (keeping the most recent).
    3. Escalates ``spec.severity`` if the merged signals contain a higher severity.
    4. Merges new categories into ``spec.categories``.

    All mutations are applied in a single ``patch_spec`` call guarded by
    ``resourceVersion`` to prevent lost-update races.  Conflicts are retried
    with exponential backoff and jitter.

    Args:
        inc_name: Incident name.
        signals: Incoming signals to merge into the incident.
        categories: Categories observed in this signal batch.
        max_signals: Maximum number of signals to retain (most recent wins).
    """
    new_entries = [s.model_dump(by_alias=True, mode="json") for s in signals]

    for attempt in range(_MAX_UPDATE_RETRIES):
        inc = await get_resource(INCIDENTS, inc_name)
        spec = inc.get("spec", {})
        existing_signals: list[dict[str, Any]] = spec.get("signals", [])
        existing_categories: list[str] = spec.get("categories", [])
        resource_version = inc.get("metadata", {}).get("resourceVersion", "")

        deduped = _dedup_signals(existing_signals, new_entries)
        merged = existing_signals + deduped
        if len(merged) > max_signals:
            merged = merged[-max_signals:]

        merged_categories = list(existing_categories)
        for c in categories:
            if c.value not in merged_categories:
                merged_categories.append(c.value)

        spec_patch: dict[str, Any] = {
            "signals": merged, "categories": merged_categories}

        current_severity = spec.get("severity", Severity.INFO.value)
        new_highest = _highest_severity_from_raw(merged)
        if _severity_index(new_highest) > _severity_index(current_severity):
            spec_patch["severity"] = new_highest

        try:
            await patch_spec(
                INCIDENTS,
                inc_name,
                spec_patch,
                resource_version=resource_version,
            )
            return
        except ApiException as exc:
            if exc.status != 409 or attempt == _MAX_UPDATE_RETRIES - 1:
                # Non-conflict patch failures, or the final exhausted retry,
                # must bubble up so callers see the underlying API error.
                raise
            await asyncio.sleep(_backoff_seconds(attempt))
            continue

    raise RuntimeError(
        "failed to update incident signals after optimistic retries")
