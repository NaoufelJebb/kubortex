"""Unit tests for the AutonomyProfile handler (validation + budget resets)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

import kopf

from kubortex.operator.handlers.autonomy import on_autonomy_profile_upsert, reset_budget_counters

from ..conftest import make_autonomy_profile_resource

NS = "kubortex-system"


def _hours_ago(n: float) -> str:
    return (datetime.now(UTC) - timedelta(hours=n)).isoformat()


def _days_ago(n: int) -> str:
    return (datetime.now(UTC) - timedelta(days=n)).isoformat()


# ---------------------------------------------------------------------------
# on_autonomy_profile_upsert
# ---------------------------------------------------------------------------


class TestOnAutonomyProfileUpsert:
    async def test_valid_spec_does_not_raise(self) -> None:
        body = make_autonomy_profile_resource("sre-standard")
        await on_autonomy_profile_upsert(body=body, name="sre-standard")  # no exception

    async def test_valid_full_spec_does_not_raise(self) -> None:
        body = make_autonomy_profile_resource(
            "full-profile",
            severities=["high", "critical"],
            rules=[{"actions": ["restart-pod"], "approval": "none"}],
        )
        await on_autonomy_profile_upsert(body=body, name="full-profile")

    async def test_invalid_spec_raises_permanent_error(self) -> None:
        body = {"spec": {"confidenceThresholds": {"autoRemediate": "not-a-float"}}}
        with pytest.raises(kopf.PermanentError, match="Invalid AutonomyProfile"):
            await on_autonomy_profile_upsert(body=body, name="bad-profile")

    async def test_empty_spec_is_valid(self) -> None:
        # All fields have defaults, so empty spec is valid
        body = {"spec": {}}
        await on_autonomy_profile_upsert(body=body, name="minimal")


# ---------------------------------------------------------------------------
# reset_budget_counters
# ---------------------------------------------------------------------------


class TestResetBudgetCounters:
    async def test_no_timestamps_sets_both_and_patches(self, mock_k8s) -> None:
        body = {"status": {"budgetUsage": {}}}
        await reset_budget_counters(body=body, name="profile-1", namespace=NS)
        mock_k8s["patch_status"].assert_awaited_once()
        usage = mock_k8s["patch_status"].call_args.args[2]["budgetUsage"]
        assert "lastResetHour" in usage
        assert "lastResetDay" in usage

    async def test_recent_hour_same_day_no_patch(self, mock_k8s) -> None:
        recent = _hours_ago(0.5)  # 30 minutes ago
        body = {
            "status": {
                "budgetUsage": {
                    "lastResetHour": recent,
                    "lastResetDay": recent,
                }
            }
        }
        await reset_budget_counters(body=body, name="profile-1", namespace=NS)
        mock_k8s["patch_status"].assert_not_awaited()

    async def test_old_hour_same_day_resets_hour_counters(self, mock_k8s) -> None:
        old_hour = _hours_ago(2)
        same_day = _hours_ago(0.5)
        body = {
            "status": {
                "budgetUsage": {
                    "podsKilledThisHour": 3,
                    "scaleUpsThisHour": 5,
                    "lastResetHour": old_hour,
                    "lastResetDay": same_day,
                }
            }
        }
        await reset_budget_counters(body=body, name="profile-1", namespace=NS)
        mock_k8s["patch_status"].assert_awaited_once()
        usage = mock_k8s["patch_status"].call_args.args[2]["budgetUsage"]
        assert usage["podsKilledThisHour"] == 0
        assert usage["scaleUpsThisHour"] == 0

    async def test_recent_hour_yesterday_resets_day_counters(self, mock_k8s) -> None:
        recent_hour = _hours_ago(0.5)
        yesterday = _days_ago(1)
        body = {
            "status": {
                "budgetUsage": {
                    "rollbacksToday": 2,
                    "lastResetHour": recent_hour,
                    "lastResetDay": yesterday,
                }
            }
        }
        await reset_budget_counters(body=body, name="profile-1", namespace=NS)
        mock_k8s["patch_status"].assert_awaited_once()
        usage = mock_k8s["patch_status"].call_args.args[2]["budgetUsage"]
        assert usage["rollbacksToday"] == 0

    async def test_old_hour_yesterday_resets_both(self, mock_k8s) -> None:
        body = {
            "status": {
                "budgetUsage": {
                    "podsKilledThisHour": 4,
                    "rollbacksToday": 2,
                    "lastResetHour": _hours_ago(2),
                    "lastResetDay": _days_ago(1),
                }
            }
        }
        await reset_budget_counters(body=body, name="profile-1", namespace=NS)
        mock_k8s["patch_status"].assert_awaited_once()
        usage = mock_k8s["patch_status"].call_args.args[2]["budgetUsage"]
        assert usage["podsKilledThisHour"] == 0
        assert usage["rollbacksToday"] == 0

    async def test_patch_targets_autonomy_profiles_plural(self, mock_k8s) -> None:
        body = {"status": {"budgetUsage": {}}}
        await reset_budget_counters(body=body, name="profile-1", namespace=NS)
        plural = mock_k8s["patch_status"].call_args.args[0]
        assert plural == "autonomyprofiles"
