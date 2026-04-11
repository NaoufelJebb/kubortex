"""Unit tests for operator/budget.py (pure helpers + async persistence)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from kubortex.operator.budget import (
    decrement_active,
    increment_usage,
    reset_if_needed,
)
from kubortex.shared.models.autonomy import BudgetUsage

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _usage(**kwargs) -> BudgetUsage:
    return BudgetUsage.model_validate(kwargs)


# ---------------------------------------------------------------------------
# reset_if_needed
# ---------------------------------------------------------------------------


class TestResetIfNeeded:
    def test_no_timestamps_resets_both(self) -> None:
        usage = _usage()
        now = datetime.now(UTC)
        result = reset_if_needed(usage, now=now)
        assert result.pods_killed_this_hour == 0
        assert result.rollbacks_today == 0
        assert result.last_reset_hour is not None
        assert result.last_reset_day is not None

    def test_same_hour_within_3600s_no_hour_reset(self) -> None:
        # Pin to minute=45 so now-30min (minute=15) stays in the same hour
        now = datetime.now(UTC).replace(minute=45, second=0, microsecond=0)
        recent = now - timedelta(minutes=30)
        usage = _usage(
            podsKilledThisHour=3,
            scaleUpsThisHour=2,
            lastResetHour=recent.isoformat(),
            lastResetDay=now.isoformat(),
        )
        result = reset_if_needed(usage, now=now)
        assert result.pods_killed_this_hour == 3
        assert result.scale_ups_this_hour == 2

    def test_same_hour_but_3600s_elapsed_resets_hour(self) -> None:
        now = datetime.now(UTC)
        exactly_an_hour_ago = now - timedelta(seconds=3600)
        usage = _usage(
            podsKilledThisHour=4,
            scaleUpsThisHour=1,
            lastResetHour=exactly_an_hour_ago.isoformat(),
            lastResetDay=now.isoformat(),
        )
        result = reset_if_needed(usage, now=now)
        assert result.pods_killed_this_hour == 0
        assert result.scale_ups_this_hour == 0

    def test_different_hour_resets_hour_counters(self) -> None:
        now = datetime.now(UTC)
        two_hours_ago = now - timedelta(hours=2)
        usage = _usage(
            podsKilledThisHour=5,
            scaleUpsThisHour=3,
            lastResetHour=two_hours_ago.isoformat(),
            lastResetDay=now.isoformat(),
        )
        result = reset_if_needed(usage, now=now)
        assert result.pods_killed_this_hour == 0
        assert result.scale_ups_this_hour == 0

    def test_same_day_no_day_reset(self) -> None:
        now = datetime.now(UTC)
        earlier_today = now - timedelta(hours=1)
        usage = _usage(
            rollbacksToday=2,
            lastResetHour=now.isoformat(),
            lastResetDay=earlier_today.isoformat(),
        )
        result = reset_if_needed(usage, now=now)
        assert result.rollbacks_today == 2

    def test_previous_day_resets_day_counter(self) -> None:
        now = datetime.now(UTC)
        yesterday = now - timedelta(days=1)
        usage = _usage(
            rollbacksToday=2,
            lastResetHour=now.isoformat(),
            lastResetDay=yesterday.isoformat(),
        )
        result = reset_if_needed(usage, now=now)
        assert result.rollbacks_today == 0

    def test_uses_utc_now_when_no_now_provided(self) -> None:
        usage = _usage()
        result = reset_if_needed(usage)
        assert result.last_reset_hour is not None
        assert result.last_reset_day is not None

    def test_updates_last_reset_hour_timestamp(self) -> None:
        now = datetime.now(UTC)
        old = now - timedelta(hours=2)
        usage = _usage(lastResetHour=old.isoformat(), lastResetDay=now.isoformat())
        result = reset_if_needed(usage, now=now)
        assert result.last_reset_hour is not None
        assert result.last_reset_hour > old


# ---------------------------------------------------------------------------
# increment_usage
# ---------------------------------------------------------------------------


class TestIncrementUsage:
    def test_restart_pod_increments_pods_killed(self) -> None:
        usage = _usage(podsKilledThisHour=2, activeRemediations=0)
        result = increment_usage("restart-pod", usage)
        assert result.pods_killed_this_hour == 3
        assert result.active_remediations == 1

    def test_rollback_deployment_increments_rollbacks(self) -> None:
        usage = _usage(rollbacksToday=1, activeRemediations=0)
        result = increment_usage("rollback-deployment", usage)
        assert result.rollbacks_today == 2
        assert result.active_remediations == 1

    def test_scale_up_increments_scale_ups(self) -> None:
        usage = _usage(scaleUpsThisHour=3, activeRemediations=1)
        result = increment_usage("scale-up", usage)
        assert result.scale_ups_this_hour == 4
        assert result.active_remediations == 2

    def test_unknown_action_only_increments_active(self) -> None:
        usage = _usage(podsKilledThisHour=0, rollbacksToday=0, scaleUpsThisHour=0, activeRemediations=0)
        result = increment_usage("other-action", usage)
        assert result.pods_killed_this_hour == 0
        assert result.rollbacks_today == 0
        assert result.scale_ups_this_hour == 0
        assert result.active_remediations == 1

    def test_does_not_mutate_original(self) -> None:
        usage = _usage(podsKilledThisHour=1)
        increment_usage("restart-pod", usage)
        assert usage.pods_killed_this_hour == 1


# ---------------------------------------------------------------------------
# decrement_active
# ---------------------------------------------------------------------------


class TestDecrementActive:
    def test_decrements_active_remediations(self) -> None:
        usage = _usage(activeRemediations=3)
        result = decrement_active(usage)
        assert result.active_remediations == 2

    def test_floors_at_zero(self) -> None:
        usage = _usage(activeRemediations=0)
        result = decrement_active(usage)
        assert result.active_remediations == 0

    def test_does_not_mutate_original(self) -> None:
        usage = _usage(activeRemediations=2)
        decrement_active(usage)
        assert usage.active_remediations == 2


