"""Pure-function policy engine for evaluating remediation actions.

Evaluation chain (first failure is terminal):
  1. Action whitelist — is the action listed in any autonomy rule?
  2. Blackout check — is a blackout window active?
  3. Budget check — would this action exceed a hard ceiling?
  4. Cooldown check — is the target under cooldown?
  5. Confidence check — does confidence meet the threshold?
  6. Approval level — ``none`` (auto-execute) or ``required``.

All functions are pure: no I/O, no side effects, no Kubernetes calls.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime

from croniter import croniter

from kubortex.shared.models.autonomy import (
    AutonomyProfileSpec,
    BlackoutWindow,
    Budgets,
    BudgetUsage,
)
from kubortex.shared.types import ApprovalLevel, Severity


@dataclass(frozen=True)
class PolicyDecision:
    """Result of evaluating a single action against an AutonomyProfile."""

    allowed: bool
    approval: ApprovalLevel = ApprovalLevel.REQUIRED
    deny_reason: str = ""
    matched_rule: str = ""
    budget_available: bool = True


@dataclass(frozen=True)
class ActionContext:
    """Minimal context needed to evaluate an action."""

    action_type: str
    severity: Severity
    confidence: float
    target_key: str = ""  # e.g. "production/frontend"


@dataclass
class CooldownState:
    """Tracks per-target last-remediation timestamps."""

    last_remediation: dict[str, datetime] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Evaluation steps (all pure)
# ---------------------------------------------------------------------------


def _check_whitelist(action: ActionContext, profile: AutonomyProfileSpec) -> str | None:
    """Return deny reason if action is not listed in any autonomy rule."""
    for rule in profile.autonomy_rules:
        if action.action_type in rule.actions:
            return None
    return f"action '{action.action_type}' is not listed in any autonomy rule"


def _check_blackout(now: datetime, windows: list[BlackoutWindow]) -> str | None:
    """Return deny reason if a blackout window is currently active."""
    for win in windows:
        it = croniter(win.cron, now)
        prev_fire: datetime = it.get_prev(datetime)
        window_end = prev_fire.timestamp() + win.duration_minutes * 60
        if now.timestamp() < window_end:
            end_iso = datetime.fromtimestamp(window_end, tz=UTC).isoformat()
            return f"blackout window '{win.name}' is active until {end_iso}"
    return None


def _check_budget(action: ActionContext, budgets: Budgets, usage: BudgetUsage) -> str | None:
    """Return deny reason if a hard budget ceiling would be exceeded."""
    if action.action_type == "restart-pod":
        if usage.pods_killed_this_hour >= budgets.max_pods_killed_per_hour:
            return "maxPodsKilledPerHour budget exhausted"
    elif action.action_type == "rollback-deployment":
        if usage.rollbacks_today >= budgets.max_rollbacks_per_day:
            return "maxRollbacksPerDay budget exhausted"
    elif action.action_type == "scale-up" and (
        usage.scale_ups_this_hour >= budgets.max_scale_ups_per_hour
    ):
        return "maxScaleUpsPerHour budget exhausted"

    if usage.active_remediations >= budgets.max_concurrent_remediations:
        return "maxConcurrentRemediations budget exhausted"
    return None


def _check_cooldown(
    action: ActionContext,
    cooldown_after_seconds: int,
    cooldown_state: CooldownState,
    now: datetime,
) -> str | None:
    """Return deny reason if the target is under cooldown."""
    last = cooldown_state.last_remediation.get(action.target_key)
    if last is None:
        return None
    elapsed = (now - last).total_seconds()
    if elapsed < cooldown_after_seconds:
        remaining = int(cooldown_after_seconds - elapsed)
        return f"target '{action.target_key}' under cooldown for {remaining}s"
    return None


def _determine_approval(action: ActionContext, profile: AutonomyProfileSpec) -> ApprovalLevel:
    """Determine the required approval level for the action."""
    for rule in profile.autonomy_rules:
        if action.action_type not in rule.actions:
            continue
        if rule.max_severity is not None:
            severity_order = list(Severity)
            if severity_order.index(action.severity) > severity_order.index(rule.max_severity):
                continue
        return rule.approval
    return ApprovalLevel.REQUIRED


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def evaluate_action(
    action: ActionContext,
    profile: AutonomyProfileSpec,
    budget_usage: BudgetUsage,
    *,
    cooldown_state: CooldownState | None = None,
    now: datetime | None = None,
) -> PolicyDecision:
    """Evaluate a proposed action against the AutonomyProfile.

    Returns a :class:`PolicyDecision` indicating whether the action is
    allowed, and if so, whether approval is required.
    """
    now = now or datetime.now(UTC)
    cooldown_state = cooldown_state or CooldownState()

    # 1. Whitelist
    reason = _check_whitelist(action, profile)
    if reason:
        return PolicyDecision(allowed=False, deny_reason=reason)

    # 2. Blackout
    reason = _check_blackout(now, profile.blackout_windows)
    if reason:
        return PolicyDecision(allowed=False, deny_reason=reason)

    # 3. Budget (hard ceiling — cannot override with approval)
    reason = _check_budget(action, profile.budgets, budget_usage)
    if reason:
        return PolicyDecision(allowed=False, deny_reason=reason, budget_available=False)

    # 4. Cooldown
    cooldown_secs = profile.cooldown.after_remediation_seconds
    reason = _check_cooldown(action, cooldown_secs, cooldown_state, now)
    if reason:
        return PolicyDecision(allowed=False, deny_reason=reason)

    # 5. Confidence check
    thresholds = profile.confidence_thresholds
    if action.confidence < thresholds.escalate:
        return PolicyDecision(
            allowed=False,
            deny_reason=(
                f"confidence {action.confidence:.2f} below escalate threshold {thresholds.escalate}"
            ),
        )

    # 6. Approval level
    approval = _determine_approval(action, profile)
    if approval == ApprovalLevel.NONE and action.confidence >= thresholds.auto_remediate:
        return PolicyDecision(
            allowed=True,
            approval=ApprovalLevel.NONE,
            matched_rule=(
                f"{action.action_type} auto-approved at confidence {action.confidence:.2f}"
            ),
        )

    return PolicyDecision(
        allowed=True,
        approval=ApprovalLevel.REQUIRED,
        matched_rule=f"{action.action_type} requires approval",
    )
