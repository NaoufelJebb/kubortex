"""Unit tests for kubortex.edge.notifications.events (domain event models)."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from kubortex.edge.notifications.events import (
    ActionExecuted,
    ActionFailed,
    ActionSucceeded,
    ApprovalRequired,
    DomainEvent,
    EscalationTriggered,
    IncidentDetected,
    IncidentResolved,
    InvestigationCompleted,
    InvestigationStarted,
    RemediationPlanned,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_NOW = datetime(2024, 1, 1, tzinfo=UTC)

_BASE = {
    "incidentName": "inc-001",
    "namespace": "default",
    "timestamp": _NOW,
    "payload": {"phase": "Detected"},
}


def _make_event(cls, **overrides):
    return cls(**{**_BASE, **overrides})


# ---------------------------------------------------------------------------
# DomainEvent base
# ---------------------------------------------------------------------------


class TestDomainEvent:
    def test_requires_event_type(self) -> None:
        with pytest.raises(ValidationError):
            DomainEvent(incidentName="x", namespace="y", timestamp=_NOW)

    def test_payload_defaults_to_empty_dict(self) -> None:
        ev = DomainEvent(
            eventType="Custom", incidentName="x", namespace="y", timestamp=_NOW
        )
        assert ev.payload == {}

    def test_populate_by_name_works(self) -> None:
        # Can use camelCase alias or snake_case field name
        ev = DomainEvent(
            event_type="Custom",
            incident_name="x",
            namespace="y",
            timestamp=_NOW,
        )
        assert ev.event_type == "Custom"
        assert ev.incident_name == "x"


# ---------------------------------------------------------------------------
# Concrete event types — default event_type values
# ---------------------------------------------------------------------------


class TestEventTypeDefaults:
    @pytest.mark.parametrize(
        ("cls", "expected_type"),
        [
            (IncidentDetected, "IncidentDetected"),
            (InvestigationStarted, "InvestigationStarted"),
            (InvestigationCompleted, "InvestigationCompleted"),
            (RemediationPlanned, "RemediationPlanned"),
            (ApprovalRequired, "ApprovalRequired"),
            (ActionExecuted, "ActionExecuted"),
            (ActionSucceeded, "ActionSucceeded"),
            (ActionFailed, "ActionFailed"),
            (IncidentResolved, "IncidentResolved"),
            (EscalationTriggered, "EscalationTriggered"),
        ],
    )
    def test_default_event_type(self, cls, expected_type: str) -> None:
        ev = _make_event(cls)
        assert ev.event_type == expected_type

    def test_incident_detected_carries_payload(self) -> None:
        ev = _make_event(IncidentDetected, payload={"severity": "critical"})
        assert ev.payload["severity"] == "critical"

    def test_incident_name_is_preserved(self) -> None:
        ev = _make_event(IncidentResolved, incidentName="my-incident")
        assert ev.incident_name == "my-incident"

    def test_timestamp_is_preserved(self) -> None:
        ev = _make_event(ActionFailed)
        assert ev.timestamp == _NOW
