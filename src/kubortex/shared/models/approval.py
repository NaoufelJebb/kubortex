"""Pydantic v2 models for the ApprovalRequest CRD (kubortex.io/v1alpha1)."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

from kubortex.shared.types import ApprovalRequestPhase, DecisionType, RiskTier

from .incident import Condition, TargetRef

# ---------------------------------------------------------------------------
# Sub-models
# ---------------------------------------------------------------------------


class ApprovalActionDetail(BaseModel):
    """Action detail embedded in the approval request for human review."""

    id: str
    type: str
    target: TargetRef
    parameters: dict[str, str | int | float | bool] = Field(default_factory=dict)
    rationale: str = ""
    risk_tier: RiskTier = Field(RiskTier.LOW, alias="riskTier")
    dry_run_result: str = Field("", alias="dryRunResult")

    model_config = {"populate_by_name": True}


class ApprovalInvestigationContext(BaseModel):
    """Minimal investigation context shown to the approver."""

    hypothesis: str = ""
    confidence: float = 0.0


# ---------------------------------------------------------------------------
# CRD spec & status
# ---------------------------------------------------------------------------


class ApprovalRequestSpec(BaseModel):
    incident_ref: str = Field(alias="incidentRef")
    investigation_ref: str = Field("", alias="investigationRef")
    remediation_plan_ref: str = Field(alias="remediationPlanRef")
    action: ApprovalActionDetail
    investigation: ApprovalInvestigationContext | None = None
    timeout_minutes: int = Field(30, alias="timeoutMinutes")

    model_config = {"populate_by_name": True}


class ApprovalRequestStatus(BaseModel):
    phase: ApprovalRequestPhase = ApprovalRequestPhase.PENDING
    decision: DecisionType | None = None
    decided_by: str = Field("", alias="decidedBy")
    decided_at: datetime | None = Field(None, alias="decidedAt")
    reason: str = ""
    conditions: list[Condition] = Field(default_factory=list)

    model_config = {"populate_by_name": True}
