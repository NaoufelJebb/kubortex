"""Budget accounting for AutonomyProfile budget usage.

Provides pure check/increment helpers and an async ``update_usage`` function
that reads, transforms, and writes budget counters atomically using optimistic
concurrency (resourceVersion compare-and-swap).  Concurrent operators or timer
handlers writing to the same profile will get a 409 Conflict on the losing
write and retry — this ensures counters converge to the correct value rather
than silently losing increments.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime

import structlog
from kubernetes_asyncio.client import ApiException

from kubortex.shared.constants import AUTONOMY_PROFILES
from kubortex.shared.crds import get_resource, patch_status
from kubortex.shared.metrics import BUDGET_REMAINING
from kubortex.shared.models.autonomy import AutonomyProfileSpec, BudgetUsage

logger = structlog.get_logger(__name__)

_MAX_RETRIES = 5


# ---------------------------------------------------------------------------
# Pure helpers (no I/O)
# ---------------------------------------------------------------------------


def reset_if_needed(usage: BudgetUsage, now: datetime | None = None) -> BudgetUsage:
    """Reset budget counters when the hour or day changes.

    Args:
        usage: Current budget usage.
        now: Optional reference time.

    Returns:
        Updated budget usage.
    """
    now = now or datetime.now(UTC)
    data = usage.model_dump(by_alias=True)

    last_hour = usage.last_reset_hour
    if last_hour is None or now.hour != last_hour.hour or (now - last_hour).total_seconds() >= 3600:
        data["podsKilledThisHour"] = 0
        data["scaleUpsThisHour"] = 0
        data["lastResetHour"] = now.isoformat()

    last_day = usage.last_reset_day
    if last_day is None or now.date() != last_day.date():
        data["rollbacksToday"] = 0
        data["lastResetDay"] = now.isoformat()

    return BudgetUsage.model_validate(data)


def increment_usage(action_type: str, usage: BudgetUsage) -> BudgetUsage:
    """Increment usage counters for an action.

    Args:
        action_type: Executed action type.
        usage: Current budget usage.

    Returns:
        Updated budget usage.
    """
    data = usage.model_dump(by_alias=True)
    if action_type == "restart-pod":
        data["podsKilledThisHour"] += 1
    elif action_type == "rollback-deployment":
        data["rollbacksToday"] += 1
    elif action_type == "scale-up":
        data["scaleUpsThisHour"] += 1
    data["activeRemediations"] += 1
    return BudgetUsage.model_validate(data)


def decrement_active(usage: BudgetUsage) -> BudgetUsage:
    """Decrement the active remediation counter.

    Args:
        usage: Current budget usage.

    Returns:
        Updated budget usage.
    """
    data = usage.model_dump(by_alias=True)
    data["activeRemediations"] = max(0, data["activeRemediations"] - 1)
    return BudgetUsage.model_validate(data)


# ---------------------------------------------------------------------------
# Async persistence (writes to K8s)
# ---------------------------------------------------------------------------


async def update_usage(
    profile_name: str,
    transform: Callable[[BudgetUsage], BudgetUsage],
) -> BudgetUsage:
    """Atomically read, transform, and write budget usage.

    Uses optimistic concurrency: the ``resourceVersion`` read from the profile
    is included in the PATCH body.  If a concurrent write happened between the
    read and the write, Kubernetes returns 409 Conflict and the operation is
    retried from scratch (up to ``_MAX_RETRIES`` times).

    This replaces the racy ``load_usage`` → ``increment_usage`` → ``persist_usage``
    pattern that could silently lose increments under concurrent plan evaluations.

    Args:
        profile_name: AutonomyProfile name.
        transform: Pure function that maps current usage to updated usage.

    Returns:
        The committed ``BudgetUsage`` after a successful write.

    Raises:
        ApiException: Re-raised if the conflict persists after all retries,
            or if the error is not a 409.
        Exception: Re-raised if the profile cannot be fetched.
    """
    for attempt in range(_MAX_RETRIES):
        resource = await get_resource(AUTONOMY_PROFILES, profile_name)
        rv = resource["metadata"]["resourceVersion"]
        raw = (resource.get("status") or {}).get("budgetUsage", {})
        current = BudgetUsage.model_validate(raw)
        updated = transform(current)
        try:
            await patch_status(
                AUTONOMY_PROFILES,
                profile_name,
                {"budgetUsage": updated.model_dump(by_alias=True)},
                resource_version=rv,
            )
            logger.info("budget_updated", profile=profile_name, attempt=attempt)
            emit_budget_remaining(profile_name, resource.get("spec", {}), updated)
            return updated
        except ApiException as exc:
            if exc.status == 409 and attempt < _MAX_RETRIES - 1:
                logger.debug("budget_conflict_retry", profile=profile_name, attempt=attempt)
                continue
            raise

    raise RuntimeError("unreachable")  # pragma: no cover


def emit_budget_remaining(
    profile_name: str,
    profile_spec: dict,
    usage: BudgetUsage,
) -> None:
    """Publish remaining budget headroom as a gauge for each budget dimension.

    Parses the profile spec through Pydantic, then computes
    ``limit - used`` for pods-killed, rollbacks, scale-ups, and active
    remediations. Failures are swallowed — observability must never break
    the caller's write path.

    Args:
        profile_name: AutonomyProfile name (gauge label).
        profile_spec: Raw spec dict from the profile resource.
        usage: The committed ``BudgetUsage`` after the write.
    """
    try:
        parsed = AutonomyProfileSpec.model_validate(profile_spec)
        budgets = parsed.budgets
    except Exception:
        return
    remaining = {
        "pods_killed_per_hour": budgets.max_pods_killed_per_hour - usage.pods_killed_this_hour,
        "rollbacks_per_day": budgets.max_rollbacks_per_day - usage.rollbacks_today,
        "scale_ups_per_hour": budgets.max_scale_ups_per_hour - usage.scale_ups_this_hour,
        "concurrent_remediations": (
            budgets.max_concurrent_remediations - usage.active_remediations
        ),
    }
    for budget_type, value in remaining.items():
        BUDGET_REMAINING.labels(profile=profile_name, budget_type=budget_type).set(value)
