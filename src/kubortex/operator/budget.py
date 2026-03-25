"""Budget accounting for AutonomyProfile budget usage.

Provides pure check/increment helpers and an async function that
persists updated counters to the AutonomyProfile status sub-resource
using optimistic concurrency.
"""

from __future__ import annotations

from datetime import UTC, datetime

import structlog

from kubortex.shared.k8s import get_resource, patch_status
from kubortex.shared.models.autonomy import Budgets, BudgetUsage

logger = structlog.get_logger(__name__)

AUTONOMY_PLURAL = "autonomyprofiles"


# ---------------------------------------------------------------------------
# Pure helpers (no I/O)
# ---------------------------------------------------------------------------


def reset_if_needed(usage: BudgetUsage, now: datetime | None = None) -> BudgetUsage:
    """Return a copy of *usage* with counters reset if the hour/day rolled over."""
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
    """Return a deny reason if the action would exceed a hard ceiling, else None."""
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
    """Return a new BudgetUsage with the appropriate counter incremented."""
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
    """Return a new BudgetUsage with activeRemediations decremented."""
    data = usage.model_dump(by_alias=True)
    data["activeRemediations"] = max(0, data["activeRemediations"] - 1)
    return BudgetUsage.model_validate(data)


# ---------------------------------------------------------------------------
# Async persistence (writes to K8s)
# ---------------------------------------------------------------------------


async def persist_usage(profile_name: str, usage: BudgetUsage) -> None:
    """Patch the AutonomyProfile status with updated budget usage."""
    await patch_status(
        AUTONOMY_PLURAL,
        profile_name,
        {"budgetUsage": usage.model_dump(by_alias=True)},
    )
    logger.info("budget_usage_persisted", profile=profile_name)


async def load_usage(profile_name: str) -> BudgetUsage:
    """Load current budget usage from AutonomyProfile status."""
    resource = await get_resource(AUTONOMY_PLURAL, profile_name)
    raw = (resource.get("status") or {}).get("budgetUsage", {})
    return BudgetUsage.model_validate(raw)
