"""Pydantic v2 models for the AutonomyProfile CRD (kubortex.io/v1alpha1)."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

from kubortex.shared.types import ApprovalLevel, Category, Severity

from .incident import Condition

# ---------------------------------------------------------------------------
# Sub-models
# ---------------------------------------------------------------------------


class NamespaceSelector(BaseModel):
    match_names: list[str] = Field(default_factory=list, alias="matchNames")
    match_labels: dict[str, str] = Field(default_factory=dict, alias="matchLabels")

    model_config = {"populate_by_name": True}


class AutonomyScope(BaseModel):
    namespaces: NamespaceSelector = Field(default_factory=NamespaceSelector)
    severities: list[Severity] = Field(default_factory=list)
    categories: list[Category] = Field(default_factory=list)

    model_config = {"populate_by_name": True}


class AutonomyRule(BaseModel):
    actions: list[str] = Field(default_factory=list)
    max_severity: Severity | None = Field(None, alias="maxSeverity")
    approval: ApprovalLevel = ApprovalLevel.REQUIRED

    model_config = {"populate_by_name": True}


class Budgets(BaseModel):
    max_concurrent_remediations: int = Field(2, alias="maxConcurrentRemediations")
    max_pods_killed_per_hour: int = Field(5, alias="maxPodsKilledPerHour")
    max_rollbacks_per_day: int = Field(3, alias="maxRollbacksPerDay")
    max_scale_ups_per_hour: int = Field(10, alias="maxScaleUpsPerHour")

    model_config = {"populate_by_name": True}


class CooldownConfig(BaseModel):
    after_remediation_seconds: int = Field(300, alias="afterRemediationSeconds")
    after_failed_seconds: int = Field(900, alias="afterFailedSeconds")

    model_config = {"populate_by_name": True}


class BlackoutWindow(BaseModel):
    name: str
    cron: str
    duration_minutes: int = Field(alias="durationMinutes")

    model_config = {"populate_by_name": True}


class ConfidenceThresholds(BaseModel):
    auto_remediate: float = Field(0.85, alias="autoRemediate")
    propose: float = 0.60
    escalate: float = 0.60

    model_config = {"populate_by_name": True}


class VerificationConfig(BaseModel):
    enabled: bool = True
    wait_after_action_seconds: int = Field(60, alias="waitAfterActionSeconds")
    rollback_on_regression: bool = Field(True, alias="rollbackOnRegression")
    success_criteria: dict[str, float] = Field(default_factory=dict, alias="successCriteria")

    model_config = {"populate_by_name": True}


# ---------------------------------------------------------------------------
# Budget usage tracking (status sub-model)
# ---------------------------------------------------------------------------


class BudgetUsage(BaseModel):
    pods_killed_this_hour: int = Field(0, alias="podsKilledThisHour")
    rollbacks_today: int = Field(0, alias="rollbacksToday")
    scale_ups_this_hour: int = Field(0, alias="scaleUpsThisHour")
    active_remediations: int = Field(0, alias="activeRemediations")
    last_reset_hour: datetime | None = Field(None, alias="lastResetHour")
    last_reset_day: datetime | None = Field(None, alias="lastResetDay")

    model_config = {"populate_by_name": True}


# ---------------------------------------------------------------------------
# CRD spec & status
# ---------------------------------------------------------------------------


class AutonomyProfileSpec(BaseModel):
    scope: AutonomyScope = Field(default_factory=AutonomyScope)
    autonomy_rules: list[AutonomyRule] = Field(default_factory=list, alias="autonomyRules")
    budgets: Budgets = Field(default_factory=Budgets)
    cooldown: CooldownConfig = Field(default_factory=CooldownConfig)
    escalation_deadline_minutes: int = Field(15, alias="escalationDeadlineMinutes")
    max_investigation_retries: int = Field(2, alias="maxInvestigationRetries")
    blackout_windows: list[BlackoutWindow] = Field(default_factory=list, alias="blackoutWindows")
    confidence_thresholds: ConfidenceThresholds = Field(
        default_factory=ConfidenceThresholds, alias="confidenceThresholds"
    )
    verification: VerificationConfig = Field(default_factory=VerificationConfig)

    model_config = {"populate_by_name": True}


class AutonomyProfileStatus(BaseModel):
    budget_usage: BudgetUsage = Field(default_factory=BudgetUsage, alias="budgetUsage")
    conditions: list[Condition] = Field(default_factory=list)

    model_config = {"populate_by_name": True}
