"""Pydantic v2 models for the RemediationPlan CRD (kubortex.io/v1alpha1).

A RemediationPlan is created by the remediator worker from an Investigation
result. It carries a list of proposed actions, each independently policy-
evaluated by the operator before an ApprovalRequest or ActionExecution is
spawned.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from kubortex.shared.types import ApprovalLevel, RemediationPlanPhase, RiskTier

from .incident import Condition, TargetRef

# ---------------------------------------------------------------------------
# Sub-models
# ---------------------------------------------------------------------------


class ActionProposal(BaseModel):
    """A single action proposed by the remediator within a RemediationPlan.

    Each proposal is independently evaluated by the policy engine. The
    ``risk_tier`` influences approval routing: low-risk actions may be
    auto-approved while high-risk ones require human sign-off depending
    on the matching AutonomyRule. ``reversible`` is a hint used by the
    operator when deciding whether to allow rollback on verification failure.
    """

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
    """Outcome of running a single ActionProposal through the policy engine.

    Written into ``RemediationPlanSpec.policyEvaluation`` by the operator
    before spawning child resources. ``allowed=False`` means the action is
    blocked (budget exhausted, blackout window, no matching rule).
    ``approval_required`` distinguishes auto-execute (``none``) from
    human-gated (``required``) when ``allowed=True``.
    """

    action_id: str = Field(alias="actionId")
    allowed: bool
    approval_required: ApprovalLevel = Field(ApprovalLevel.REQUIRED, alias="approvalRequired")
    matched_rule: str = Field("", alias="matchedRule")
    budget_available: bool = Field(True, alias="budgetAvailable")

    model_config = {"populate_by_name": True}


class VerificationMetric(BaseModel):
    """PromQL-based check used to verify that a remediation action had the intended effect.

    After an action executes, the remediator waits ``check_delay_seconds``
    then queries ``promql``. If the result exceeds ``success_threshold``
    the action is considered successful. If the profile has
    ``rollbackOnRegression=True`` and the metric regresses, a rollback
    is triggered automatically.
    """

    promql: str
    success_threshold: float = Field(alias="successThreshold")
    check_delay_seconds: int = Field(60, alias="checkDelaySeconds")

    model_config = {"populate_by_name": True}


# ---------------------------------------------------------------------------
# CRD spec & status
# ---------------------------------------------------------------------------


class RemediationPlanSpec(BaseModel):
    """Spec of a RemediationPlan CRD, as written by the remediator worker.

    ``policy_evaluation`` is populated by the operator after it reads the
    plan — it records the per-action allow/approval decisions before any
    ApprovalRequest or ActionExecution is created.
    """

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
    """Operator-managed status of a RemediationPlan.

    ``approval_request_refs`` and ``action_execution_refs`` are populated
    as the operator spawns child resources for each action in the plan.
    A plan may produce both: auto-approved actions create ActionExecutions
    directly while gated actions create ApprovalRequests first.
    """

    phase: RemediationPlanPhase = RemediationPlanPhase.PROPOSED
    approval_request_refs: list[str] = Field(default_factory=list, alias="approvalRequestRefs")
    action_execution_refs: list[str] = Field(default_factory=list, alias="actionExecutionRefs")
    conditions: list[Condition] = Field(default_factory=list)

    model_config = {"populate_by_name": True}
