"""Pydantic v2 models for the Investigation CRD (kubortex.io/v1alpha1)."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

from kubortex.shared.types import Category, InvestigationPhase, Severity

from .incident import Condition, Signal, TargetRef

# ---------------------------------------------------------------------------
# Sub-models
# ---------------------------------------------------------------------------


class DiagnosticHints(BaseModel):
    """Hints from the learning system injected into investigation spec."""

    preferred_skill_order: list[str] = Field(default_factory=list, alias="preferredSkillOrder")
    avoid_paths: list[str] = Field(default_factory=list, alias="avoidPaths")

    model_config = {"populate_by_name": True}


class EvidenceItem(BaseModel):
    """A single piece of evidence gathered during investigation."""

    skill: str
    query: str
    value_summary: str = Field("", alias="valueSummary")
    interpretation: str = ""
    payload_ref: str = Field("", alias="payloadRef")

    model_config = {"populate_by_name": True}


class RecommendedAction(BaseModel):
    """An action proposed by the investigator."""

    type: str
    target: TargetRef
    parameters: dict[str, str | int | float | bool] = Field(default_factory=dict)
    rationale: str = ""


class DiagnosticPathEntry(BaseModel):
    """Record of a single step in the diagnostic path for the learning system."""

    skill: str
    query: str
    was_useful: bool = Field(False, alias="wasUseful")

    model_config = {"populate_by_name": True}


class InvestigationResult(BaseModel):
    """Structured output produced by the conclude node."""

    hypothesis: str
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning: str = ""
    evidence: list[EvidenceItem] = Field(default_factory=list)
    recommended_actions: list[RecommendedAction] = Field(
        default_factory=list, alias="recommendedActions"
    )
    escalate: bool = False
    escalation_reason: str | None = Field(None, alias="escalationReason")
    diagnostic_path: list[DiagnosticPathEntry] = Field(default_factory=list, alias="diagnosticPath")

    model_config = {"populate_by_name": True}


class SkillInvocationRecord(BaseModel):
    """Telemetry for a single skill invocation."""

    skill: str
    latency_ms: float = Field(0.0, alias="latencyMs")
    output_size: int = Field(0, alias="outputSize")
    error: str | None = None

    model_config = {"populate_by_name": True}


class InvestigationTelemetry(BaseModel):
    """Telemetry data tracked during investigation."""

    iterations_used: int = Field(0, alias="iterationsUsed")
    llm_calls: int = Field(0, alias="llmCalls")
    compression_events: list[str] = Field(default_factory=list, alias="compressionEvents")
    skill_invocations: list[SkillInvocationRecord] = Field(
        default_factory=list, alias="skillInvocations"
    )

    model_config = {"populate_by_name": True}


# ---------------------------------------------------------------------------
# CRD spec & status
# ---------------------------------------------------------------------------


class PriorAttempt(BaseModel):
    """Context from a previous failed investigation attempt."""

    hypothesis: str = ""
    failure_reason: str = Field("", alias="failureReason")

    model_config = {"populate_by_name": True}


class InvestigationSpec(BaseModel):
    incident_ref: str = Field(alias="incidentRef")
    category: Category
    severity: Severity
    summary: str
    target_ref: TargetRef | None = Field(None, alias="targetRef")
    signals: list[Signal] = Field(default_factory=list)
    prior_attempts: list[PriorAttempt] = Field(default_factory=list, alias="priorAttempts")
    diagnostic_hints: DiagnosticHints | None = Field(None, alias="diagnosticHints")
    max_iterations: int = Field(10, alias="maxIterations")
    timeout_seconds: int = Field(300, alias="timeoutSeconds")

    model_config = {"populate_by_name": True}


class InvestigationStatus(BaseModel):
    phase: InvestigationPhase = InvestigationPhase.PENDING
    claimed_by: str = Field("", alias="claimedBy")
    claimed_at: datetime | None = Field(None, alias="claimedAt")
    selected_runbook: str | None = Field(None, alias="selectedRunbook")
    result: InvestigationResult | None = None
    telemetry: InvestigationTelemetry = Field(default_factory=InvestigationTelemetry)
    conditions: list[Condition] = Field(default_factory=list)

    model_config = {"populate_by_name": True}
