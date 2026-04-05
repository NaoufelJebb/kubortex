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

from kubortex.shared.crds import get_resource, patch_status
from kubortex.shared.models.autonomy import Budgets, BudgetUsage

logger = structlog.get_logger(__name__)

AUTONOMY_PLURAL = "autonomyprofiles"
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


def check_budget(action_type: str, budgets: Budgets, usage: BudgetUsage) -> str | None:
    """Check whether an action exceeds budget limits.

    Args:
        action_type: Action type to evaluate.
        budgets: Budget limits.
        usage: Current budget usage.

    Returns:
        Deny reason, or ``None`` when within budget.
    """
    if action_type == "restart-pod":
        if usage.pods_killed_this_hour >= budgets.max_pods_killed_per_hour:
            return "maxPodsKilledPerHour exhausted"
    elif action_type == "rollback-deployment":
        if usage.rollbacks_today >= budgets.max_rollbacks_per_day:
            return "maxRollbacksPerDay exhausted"
    elif action_type == "scale-up" and usage.scale_ups_this_hour >= budgets.max_scale_ups_per_hour:
        return "maxScaleUpsPerHour exhausted"
    if usage.active_remediations >= budgets.max_concurrent_remediations:
        return "maxConcurrentRemediations exhausted"
    return None


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
        resource = await get_resource(AUTONOMY_PLURAL, profile_name)
        rv = resource["metadata"]["resourceVersion"]
        raw = (resource.get("status") or {}).get("budgetUsage", {})
        current = BudgetUsage.model_validate(raw)
        updated = transform(current)
        try:
            await patch_status(
                AUTONOMY_PLURAL,
                profile_name,
                {"budgetUsage": updated.model_dump(by_alias=True)},
                resource_version=rv,
            )
            logger.info("budget_updated", profile=profile_name, attempt=attempt)
            return updated
        except ApiException as exc:
            if exc.status == 409 and attempt < _MAX_RETRIES - 1:
                logger.debug("budget_conflict_retry", profile=profile_name, attempt=attempt)
                continue
            raise

    raise RuntimeError("unreachable")  # pragma: no cover


async def load_usage(profile_name: str) -> BudgetUsage:
    """Load budget usage from an autonomy profile (read-only).

    Prefer ``update_usage`` for any read-modify-write pattern.

    Args:
        profile_name: AutonomyProfile name.

    Returns:
        Current budget usage.
    """
    resource = await get_resource(AUTONOMY_PLURAL, profile_name)
    raw = (resource.get("status") or {}).get("budgetUsage", {})
    return BudgetUsage.model_validate(raw)


async def persist_usage(profile_name: str, usage: BudgetUsage) -> None:
    """Persist budget usage unconditionally (no conflict detection).

    Use only for decrement-on-completion paths where the counter can only
    go down and a lost write is self-correcting over time.  For increment
    paths, use ``update_usage`` instead.

    Args:
        profile_name: AutonomyProfile name.
        usage: Budget usage to persist.
    """
    await patch_status(
        AUTONOMY_PLURAL,
        profile_name,
        {"budgetUsage": usage.model_dump(by_alias=True)},
    )
    logger.info("budget_usage_persisted", profile=profile_name)
