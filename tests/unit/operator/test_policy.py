"""Unit tests for the operator policy engine (pure functions, no I/O)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from kubortex.operator.policy import (
    ActionContext,
    CooldownState,
    _check_blackout,
    _check_budget,
    _check_cooldown,
    _check_whitelist,
    _resolve_approval_level,
    evaluate_action,
)
from kubortex.shared.models.autonomy import (
    AutonomyProfileSpec,
    BlackoutWindow,
    Budgets,
    BudgetUsage,
    ConfidenceThresholds,
    CooldownConfig,
)
from kubortex.shared.types import ApprovalLevel, Severity

from .conftest import make_action_context, make_budget_usage

# Fixed evaluation time used across tests
_NOW = datetime(2026, 3, 28, 10, 0, 30, tzinfo=UTC)  # Saturday 10:00:30 UTC


def _profile(**kwargs) -> AutonomyProfileSpec:
    """Build an AutonomyProfileSpec from keyword overrides."""
    return AutonomyProfileSpec.model_validate(kwargs)


# ---------------------------------------------------------------------------
# _check_whitelist
# ---------------------------------------------------------------------------


class TestCheckWhitelist:
    def _profile_with_rules(self, actions: list[str]) -> AutonomyProfileSpec:
        return _profile(autonomyRules=[{"actions": actions, "approval": "none"}])

    def test_action_in_rule_returns_none(self) -> None:
        profile = self._profile_with_rules(["restart-pod", "scale-up"])
        assert _check_whitelist(make_action_context("restart-pod"), profile) is None

    def test_action_not_in_any_rule_returns_denial(self) -> None:
        profile = self._profile_with_rules(["scale-up"])
        reason = _check_whitelist(make_action_context("restart-pod"), profile)
        assert reason is not None
        assert "restart-pod" in reason

    def test_empty_rules_denies_all(self) -> None:
        profile = _profile()
        reason = _check_whitelist(make_action_context("restart-pod"), profile)
        assert reason is not None

    def test_action_matched_in_second_rule(self) -> None:
        profile = _profile(
            autonomyRules=[
                {"actions": ["scale-up"], "approval": "none"},
                {"actions": ["restart-pod"], "approval": "required"},
            ]
        )
        assert _check_whitelist(make_action_context("restart-pod"), profile) is None


# ---------------------------------------------------------------------------
# _check_blackout
# ---------------------------------------------------------------------------


class TestCheckBlackout:
    def test_no_windows_returns_none(self) -> None:
        assert _check_blackout(_NOW, []) is None

    def test_active_window_returns_denial(self) -> None:
        # Every-minute cron: last fired at 10:00:00, duration 2m → active until 10:02:00
        win = BlackoutWindow(name="maintenance", cron="* * * * *", durationMinutes=2)
        reason = _check_blackout(_NOW, [win])
        assert reason is not None
        assert "maintenance" in reason

    def test_elapsed_window_returns_none(self) -> None:
        # 8am daily cron: last fired at 08:00:00, duration 30m → ended at 08:30:00
        win = BlackoutWindow(name="overnight", cron="0 8 * * *", durationMinutes=30)
        assert _check_blackout(_NOW, [win]) is None

    def test_first_active_window_short_circuits(self) -> None:
        active = BlackoutWindow(name="active-win", cron="* * * * *", durationMinutes=2)
        elapsed = BlackoutWindow(name="elapsed-win", cron="0 8 * * *", durationMinutes=30)
        reason = _check_blackout(_NOW, [active, elapsed])
        assert reason is not None
        assert "active-win" in reason


# ---------------------------------------------------------------------------
# _check_budget
# ---------------------------------------------------------------------------


class TestCheckBudget:
    def _budgets(self, **kwargs) -> Budgets:
        return Budgets.model_validate(kwargs)

    def test_restart_pod_at_limit_denied(self) -> None:
        budgets = self._budgets(maxPodsKilledPerHour=5)
        usage = make_budget_usage(pods_killed=5)
        reason = _check_budget(make_action_context("restart-pod"), budgets, usage)
        assert reason is not None
        assert "maxPodsKilledPerHour" in reason

    def test_restart_pod_under_limit_allowed(self) -> None:
        budgets = self._budgets(maxPodsKilledPerHour=5)
        usage = make_budget_usage(pods_killed=4)
        assert _check_budget(make_action_context("restart-pod"), budgets, usage) is None

    def test_rollback_at_limit_denied(self) -> None:
        budgets = self._budgets(maxRollbacksPerDay=3)
        usage = make_budget_usage(rollbacks=3)
        reason = _check_budget(make_action_context("rollback-deployment"), budgets, usage)
        assert reason is not None
        assert "maxRollbacksPerDay" in reason

    def test_scale_up_at_limit_denied(self) -> None:
        budgets = self._budgets(maxScaleUpsPerHour=10)
        usage = make_budget_usage(scale_ups=10)
        reason = _check_budget(make_action_context("scale-up"), budgets, usage)
        assert reason is not None
        assert "maxScaleUpsPerHour" in reason

    def test_concurrent_remediations_at_limit_denied(self) -> None:
        budgets = self._budgets(maxConcurrentRemediations=2)
        usage = make_budget_usage(active=2)
        reason = _check_budget(make_action_context("restart-pod"), budgets, usage)
        assert reason is not None
        assert "maxConcurrentRemediations" in reason

    def test_all_under_limits_returns_none(self) -> None:
        budgets = Budgets()
        usage = make_budget_usage()
        assert _check_budget(make_action_context("restart-pod"), budgets, usage) is None

    def test_unknown_action_type_only_checks_concurrent(self) -> None:
        budgets = Budgets()
        usage = make_budget_usage(active=0)
        assert _check_budget(make_action_context("cordon-node"), budgets, usage) is None


# ---------------------------------------------------------------------------
# _check_cooldown
# ---------------------------------------------------------------------------


class TestCheckCooldown:
    def test_no_prior_remediation_returns_none(self) -> None:
        state = CooldownState()
        assert _check_cooldown(make_action_context(), 300, state, _NOW) is None

    def test_elapsed_past_cooldown_returns_none(self) -> None:
        state = CooldownState(
            last_remediation={"default/my-pod": _NOW - timedelta(seconds=400)}
        )
        assert _check_cooldown(make_action_context(), 300, state, _NOW) is None

    def test_within_cooldown_returns_denial(self) -> None:
        state = CooldownState(
            last_remediation={"default/my-pod": _NOW - timedelta(seconds=60)}
        )
        reason = _check_cooldown(make_action_context(), 300, state, _NOW)
        assert reason is not None
        assert "default/my-pod" in reason
        assert "240s" in reason  # 300 - 60 = 240

    def test_different_target_not_affected(self) -> None:
        state = CooldownState(
            last_remediation={"production/other": _NOW - timedelta(seconds=60)}
        )
        assert _check_cooldown(make_action_context(), 300, state, _NOW) is None


# ---------------------------------------------------------------------------
# _resolve_approval_level
# ---------------------------------------------------------------------------


class TestDetermineApproval:
    def test_action_in_rule_no_max_severity_returns_rule_approval(self) -> None:
        profile = _profile(
            autonomyRules=[{"actions": ["restart-pod"], "approval": "none"}]
        )
        assert _resolve_approval_level(make_action_context("restart-pod"), profile) == ApprovalLevel.NONE

    def test_action_in_rule_required_approval(self) -> None:
        profile = _profile(
            autonomyRules=[{"actions": ["rollback-deployment"], "approval": "required"}]
        )
        result = _resolve_approval_level(make_action_context("rollback-deployment"), profile)
        assert result == ApprovalLevel.REQUIRED

    def test_severity_exceeds_max_severity_falls_through_to_required(self) -> None:
        # Rule allows up to HIGH, but action severity is CRITICAL → skip rule → REQUIRED
        profile = _profile(
            autonomyRules=[
                {"actions": ["restart-pod"], "maxSeverity": "high", "approval": "none"}
            ]
        )
        ctx = make_action_context("restart-pod", severity=Severity.CRITICAL)
        assert _resolve_approval_level(ctx, profile) == ApprovalLevel.REQUIRED

    def test_severity_within_max_severity_uses_rule_approval(self) -> None:
        profile = _profile(
            autonomyRules=[
                {"actions": ["restart-pod"], "maxSeverity": "high", "approval": "none"}
            ]
        )
        ctx = make_action_context("restart-pod", severity=Severity.HIGH)
        assert _resolve_approval_level(ctx, profile) == ApprovalLevel.NONE

    def test_action_not_in_any_rule_returns_required(self) -> None:
        profile = _profile(
            autonomyRules=[{"actions": ["scale-up"], "approval": "none"}]
        )
        assert _resolve_approval_level(make_action_context("restart-pod"), profile) == ApprovalLevel.REQUIRED

    def test_empty_rules_returns_required(self) -> None:
        assert _resolve_approval_level(make_action_context(), _profile()) == ApprovalLevel.REQUIRED


# ---------------------------------------------------------------------------
# evaluate_action (full chain integration)
# ---------------------------------------------------------------------------


class TestEvaluateAction:
    """Tests the six-step evaluation chain end-to-end."""

    def _allow_profile(
        self,
        action_type: str = "restart-pod",
        approval: str = "none",
        auto_remediate: float = 0.5,
        escalate: float = 0.3,
    ) -> AutonomyProfileSpec:
        return AutonomyProfileSpec.model_validate(
            {
                "autonomyRules": [{"actions": [action_type], "approval": approval}],
                "confidenceThresholds": {
                    "autoRemediate": auto_remediate,
                    "propose": escalate,
                    "escalate": escalate,
                },
            }
        )

    def test_not_whitelisted_denied(self) -> None:
        profile = _profile()  # no rules
        decision = evaluate_action(make_action_context(), profile, make_budget_usage(), now=_NOW)
        assert not decision.allowed

    def test_blackout_active_denied(self) -> None:
        profile = self._allow_profile()
        # Add an active blackout window to the profile
        profile_dict = profile.model_dump(by_alias=True, mode="json")
        profile_dict["blackoutWindows"] = [
            {"name": "maint", "cron": "* * * * *", "durationMinutes": 2}
        ]
        profile = AutonomyProfileSpec.model_validate(profile_dict)
        decision = evaluate_action(make_action_context(), profile, make_budget_usage(), now=_NOW)
        assert not decision.allowed
        assert "maint" in decision.deny_reason

    def test_budget_exhausted_denied_with_flag(self) -> None:
        profile = self._allow_profile()
        usage = make_budget_usage(pods_killed=5)  # at default limit of 5
        # Default budgets.max_pods_killed_per_hour = 5
        decision = evaluate_action(make_action_context(), profile, usage, now=_NOW)
        assert not decision.allowed
        assert decision.budget_available is False

    def test_cooldown_active_denied(self) -> None:
        profile = self._allow_profile()
        state = CooldownState(
            last_remediation={"default/my-pod": _NOW - timedelta(seconds=60)}
        )
        decision = evaluate_action(
            make_action_context(), profile, make_budget_usage(), cooldown_state=state, now=_NOW
        )
        assert not decision.allowed
        assert "cooldown" in decision.deny_reason

    def test_confidence_below_escalate_threshold_denied(self) -> None:
        profile = self._allow_profile(escalate=0.7)
        ctx = make_action_context(confidence=0.5)
        decision = evaluate_action(ctx, profile, make_budget_usage(), now=_NOW)
        assert not decision.allowed
        assert "confidence" in decision.deny_reason

    def test_high_confidence_auto_approved(self) -> None:
        profile = self._allow_profile(approval="none", auto_remediate=0.85, escalate=0.6)
        ctx = make_action_context(confidence=0.9)
        decision = evaluate_action(ctx, profile, make_budget_usage(), now=_NOW)
        assert decision.allowed
        assert decision.approval == ApprovalLevel.NONE

    def test_medium_confidence_approval_none_rule_still_requires_approval(self) -> None:
        # confidence above escalate but below auto_remediate → allowed but REQUIRED
        profile = self._allow_profile(approval="none", auto_remediate=0.85, escalate=0.6)
        ctx = make_action_context(confidence=0.70)
        decision = evaluate_action(ctx, profile, make_budget_usage(), now=_NOW)
        assert decision.allowed
        assert decision.approval == ApprovalLevel.REQUIRED

    def test_required_approval_rule_always_requires_approval(self) -> None:
        profile = self._allow_profile(approval="required", auto_remediate=0.5, escalate=0.3)
        ctx = make_action_context(confidence=0.95)
        decision = evaluate_action(ctx, profile, make_budget_usage(), now=_NOW)
        assert decision.allowed
        assert decision.approval == ApprovalLevel.REQUIRED

    def test_default_now_used_when_not_provided(self) -> None:
        profile = self._allow_profile()
        decision = evaluate_action(make_action_context(), profile, make_budget_usage())
        assert decision.allowed  # no blackout, no cooldown → allowed

    def test_default_cooldown_state_used_when_not_provided(self) -> None:
        profile = self._allow_profile()
        decision = evaluate_action(
            make_action_context(), profile, make_budget_usage(), now=_NOW
        )
        assert decision.allowed  # empty cooldown state → no cooldown denial
