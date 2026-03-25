"""Pydantic v2 models for the ActionExecution CRD (kubortex.io/v1alpha1)."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

from kubortex.shared.types import ActionExecutionPhase, RiskTier

from .incident import Condition, TargetRef
from .remediation import VerificationMetric

# ---------------------------------------------------------------------------
# Sub-models
# ---------------------------------------------------------------------------


class ActionDetail(BaseModel):
    """The action to be executed."""

    type: str
    target: TargetRef
    parameters: dict[str, str | int | float | bool] = Field(default_factory=dict)
    risk_tier: RiskTier = Field(RiskTier.LOW, alias="riskTier")

    model_config = {"populate_by_name": True}


class ApprovalRecord(BaseModel):
    """Record of who approved the action and when."""

    decided_by: str = Field("", alias="decidedBy")
    decided_at: datetime | None = Field(None, alias="decidedAt")

    model_config = {"populate_by_name": True}


class VerificationResult(BaseModel):
    """Post-action verification outcome."""

    checked_at: datetime | None = Field(None, alias="checkedAt")
    metrics_before: dict[str, float] = Field(default_factory=dict, alias="metricsBefore")
    metrics_after: dict[str, float] = Field(default_factory=dict, alias="metricsAfter")
    improved: bool | None = None

    model_config = {"populate_by_name": True}


class RollbackResult(BaseModel):
    """Rollback execution outcome."""

    triggered: bool = False
    executed_at: datetime | None = Field(None, alias="executedAt")
    result: str | None = None

    model_config = {"populate_by_name": True}


# ---------------------------------------------------------------------------
# CRD spec & status
# ---------------------------------------------------------------------------


class ActionExecutionSpec(BaseModel):
    incident_ref: str = Field(alias="incidentRef")
    remediation_plan_ref: str = Field(alias="remediationPlanRef")
    approval_request_ref: str = Field("", alias="approvalRequestRef")
    action: ActionDetail
    approval: ApprovalRecord | None = None
    verification_metric: VerificationMetric | None = Field(None, alias="verificationMetric")
    rollback_on_regression: bool = Field(True, alias="rollbackOnRegression")

    model_config = {"populate_by_name": True}


class ActionExecutionStatus(BaseModel):
    phase: ActionExecutionPhase = ActionExecutionPhase.APPROVED
    claimed_by: str = Field("", alias="claimedBy")
    claimed_at: datetime | None = Field(None, alias="claimedAt")
    pre_flight_result: str | None = Field(None, alias="preFlightResult")
    dry_run_result: str | None = Field(None, alias="dryRunResult")
    executed_at: datetime | None = Field(None, alias="executedAt")
    completed_at: datetime | None = Field(None, alias="completedAt")
    result: str | None = None
    error: str | None = None
    verification: VerificationResult = Field(default_factory=VerificationResult)
    rollback: RollbackResult = Field(default_factory=RollbackResult)
    conditions: list[Condition] = Field(default_factory=list)

    model_config = {"populate_by_name": True}
