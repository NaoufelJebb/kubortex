"""Domain event types derived from CRD phase transitions.

Each event captures the essential information a notification sink needs
to render a human-readable message.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class DomainEvent(BaseModel):
    """Base domain event emitted by the event projector."""

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


class ActionExecuted(DomainEvent):
    event_type: str = Field(default="ActionExecuted", alias="eventType")


class ActionSucceeded(DomainEvent):
    event_type: str = Field(default="ActionSucceeded", alias="eventType")


class ActionFailed(DomainEvent):
    event_type: str = Field(default="ActionFailed", alias="eventType")


class IncidentResolved(DomainEvent):
    event_type: str = Field(default="IncidentResolved", alias="eventType")


class EscalationTriggered(DomainEvent):
    event_type: str = Field(default="EscalationTriggered", alias="eventType")
