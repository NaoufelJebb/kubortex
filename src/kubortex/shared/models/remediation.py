"""Pydantic v2 models for the RemediationPlan CRD (kubortex.io/v1alpha1)."""

from __future__ import annotations

from pydantic import BaseModel, Field

from kubortex.shared.types import ApprovalLevel, RemediationPlanPhase, RiskTier

from .incident import Condition, TargetRef

# ---------------------------------------------------------------------------
# Sub-models
# ---------------------------------------------------------------------------


class ActionProposal(BaseModel):
    """A single proposed remediation action within a plan."""

    id: str
    type: str
    target: TargetRef
    parameters: dict[str, str | int | float | bool] = Field(default_factory=dict)
    rationale: str = ""
    risk_tier: RiskTier = Field(RiskTier.LOW, alias="riskTier")
    expected_effect: str = Field("", alias="expectedEffect")
    reversible: bool = True

    model_config = {"populate_by_name": True}


class PolicyEvaluationResult(BaseModel):
    """Result of evaluating a single action against the autonomy profile."""

    action_id: str = Field(alias="actionId")
    allowed: bool
    approval_required: ApprovalLevel = Field(ApprovalLevel.REQUIRED, alias="approvalRequired")
    matched_rule: str = Field("", alias="matchedRule")
    budget_available: bool = Field(True, alias="budgetAvailable")

    model_config = {"populate_by_name": True}


class VerificationMetric(BaseModel):
    """Post-remediation verification metric."""

    promql: str
    success_threshold: float = Field(alias="successThreshold")
    check_delay_seconds: int = Field(60, alias="checkDelaySeconds")

    model_config = {"populate_by_name": True}


# ---------------------------------------------------------------------------
# CRD spec & status
# ---------------------------------------------------------------------------


class RemediationPlanSpec(BaseModel):
    incident_ref: str = Field(alias="incidentRef")
    investigation_ref: str = Field(alias="investigationRef")
    hypothesis: str = ""
    confidence: float = 0.0
    actions: list[ActionProposal] = Field(default_factory=list)
    policy_evaluation: list[PolicyEvaluationResult] = Field(
        default_factory=list, alias="policyEvaluation"
    )
    verification_metric: VerificationMetric | None = Field(None, alias="verificationMetric")

    model_config = {"populate_by_name": True}


class RemediationPlanStatus(BaseModel):
    phase: RemediationPlanPhase = RemediationPlanPhase.PROPOSED
    approval_request_refs: list[str] = Field(default_factory=list, alias="approvalRequestRefs")
    action_execution_refs: list[str] = Field(default_factory=list, alias="actionExecutionRefs")
    conditions: list[Condition] = Field(default_factory=list)

    model_config = {"populate_by_name": True}
