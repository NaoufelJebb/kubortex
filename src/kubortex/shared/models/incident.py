"""Pydantic v2 models for the Incident CRD (kubortex.io/v1alpha1)."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

from kubortex.shared.types import Category, IncidentPhase, Severity

# ---------------------------------------------------------------------------
# Shared sub-models
# ---------------------------------------------------------------------------


class TargetRef(BaseModel):
    """Reference to the Kubernetes resource under investigation."""

    kind: str
    namespace: str
    name: str


class Signal(BaseModel):
    """A single normalised alert signal snapshot."""

    alertname: str
    severity: Severity
    summary: str
    observed_at: datetime = Field(alias="observedAt")
    payload: dict[str, str] = Field(default_factory=dict)

    model_config = {"populate_by_name": True}


class TimelineEntry(BaseModel):
    """Timestamped event in the Incident timeline."""

    timestamp: datetime
    event: str
    detail: str


class Condition(BaseModel):
    """Standard Kubernetes-style status condition."""

    type: str
    status: str
    reason: str = ""
    message: str = ""
    last_transition_time: datetime | None = Field(None, alias="lastTransitionTime")

    model_config = {"populate_by_name": True}


class InvestigationSynopsis(BaseModel):
    """Compact investigation summary stored on Incident status."""

    hypothesis: str = ""
    confidence: float = 0.0
    evidence_count: int = Field(0, alias="evidenceCount")
    proposed_action_count: int = Field(0, alias="proposedActionCount")

    model_config = {"populate_by_name": True}


# ---------------------------------------------------------------------------
# CRD spec & status
# ---------------------------------------------------------------------------


class IncidentSpec(BaseModel):
    severity: Severity
    categories: list[Category] = Field(default_factory=list)
    summary: str
    source: str = ""
    signals: list[Signal] = Field(default_factory=list)
    target_ref: TargetRef | None = Field(None, alias="targetRef")

    model_config = {"populate_by_name": True}


class IncidentStatus(BaseModel):
    phase: IncidentPhase = IncidentPhase.DETECTED
    autonomy_profile: str = Field("", alias="autonomyProfile")
    investigation_ref: str = Field("", alias="investigationRef")
    remediation_plan_ref: str = Field("", alias="remediationPlanRef")
    investigation: InvestigationSynopsis | None = None
    escalation_deadline: datetime | None = Field(None, alias="escalationDeadline")
    retry_count: int = Field(0, alias="retryCount")
    max_retries: int = Field(2, alias="maxRetries")
    resolved_at: datetime | None = Field(None, alias="resolvedAt")
    timeline: list[TimelineEntry] = Field(default_factory=list)

    model_config = {"populate_by_name": True}
