"""Shared Edge notification event types derived from CRD phase transitions.

Each event captures the essential information a notification sink needs
to render a human-readable message.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, TypedDict

from pydantic import BaseModel, Field


class CommonPayload(TypedDict, total=False):
    resourceName: str
    phase: str


class IncidentPayload(CommonPayload, total=False):
    summary: str
    severity: str
    category: str
    targetKind: str
    targetNamespace: str
    targetName: str
    confidence: float
    hypothesis: str
    evidenceCount: int
    proposedActionCount: int


class InvestigationPayload(IncidentPayload, total=False):
    pass


class ApprovalPayload(CommonPayload, total=False):
    actionType: str
    remediationPlanRef: str
    targetKind: str
    targetNamespace: str
    targetName: str


class ActionPayload(ApprovalPayload, total=False):
    approvalRequestRef: str
    improved: bool


class DomainEvent(BaseModel):
    """Notification event projected from a CRD state change."""

    event_type: str = Field(alias="eventType")
    incident_name: str = Field(alias="incidentName")
    namespace: str
    timestamp: datetime
    payload: dict[str, Any] = Field(default_factory=dict)

    model_config = {"populate_by_name": True}


class IncidentDetected(DomainEvent):
    event_type: str = Field(default="IncidentDetected", alias="eventType")


class InvestigationStarted(DomainEvent):
    event_type: str = Field(default="InvestigationStarted", alias="eventType")


class InvestigationCompleted(DomainEvent):
    event_type: str = Field(default="InvestigationCompleted", alias="eventType")


class RemediationPlanned(DomainEvent):
    event_type: str = Field(default="RemediationPlanned", alias="eventType")


class ApprovalRequired(DomainEvent):
    event_type: str = Field(default="ApprovalRequired", alias="eventType")


class ApprovalRejected(DomainEvent):
    event_type: str = Field(default="ApprovalRejected", alias="eventType")


class ApprovalTimedOut(DomainEvent):
    event_type: str = Field(default="ApprovalTimedOut", alias="eventType")


class ActionExecuted(DomainEvent):
    event_type: str = Field(default="ActionExecuted", alias="eventType")


class ActionSucceeded(DomainEvent):
    event_type: str = Field(default="ActionSucceeded", alias="eventType")


class ActionFailed(DomainEvent):
    event_type: str = Field(default="ActionFailed", alias="eventType")


class IncidentFailed(DomainEvent):
    event_type: str = Field(default="IncidentFailed", alias="eventType")


class IncidentResolved(DomainEvent):
    event_type: str = Field(default="IncidentResolved", alias="eventType")


class EscalationTriggered(DomainEvent):
    event_type: str = Field(default="EscalationTriggered", alias="eventType")
