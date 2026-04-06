"""Unit tests for kubortex.edge.core.projector.EventProjector."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import ClassVar
from unittest.mock import AsyncMock

import pytest

from kubortex.edge.core.events import (
    ActionExecuted,
    ActionFailed,
    ActionSucceeded,
    ApprovalRejected,
    ApprovalRequired,
    ApprovalTimedOut,
    EscalationTriggered,
    IncidentDetected,
    IncidentFailed,
    IncidentResolved,
    InvestigationCompleted,
    InvestigationStarted,
    RemediationPlanned,
)
from kubortex.edge.core.projector import EventProjector
from kubortex.shared.config import EdgeSettings

# ---------------------------------------------------------------------------
# Fixtures & helpers
# ---------------------------------------------------------------------------


@pytest.fixture()
def projector() -> EventProjector:
    settings = EdgeSettings()
    return EventProjector(settings)


def _make_obj(
    uid: str = "uid-1",
    name: str = "res-1",
    namespace: str = "default",
    phase: str = "Detected",
    incident_name_in_spec: str | None = None,
    owner_incident: str | None = None,
    spec: dict | None = None,
    status: dict | None = None,
) -> dict:
    obj: dict = {
        "metadata": {
            "uid": uid,
            "name": name,
            "namespace": namespace,
            "ownerReferences": [],
            "resourceVersion": "1",
        },
        "spec": spec or {},
        "status": status or {"phase": phase},
    }
    if incident_name_in_spec:
        obj["spec"]["incidentRef"] = incident_name_in_spec
    if owner_incident:
        obj["metadata"]["ownerReferences"].append({"kind": "Incident", "name": owner_incident})
    return obj


# ---------------------------------------------------------------------------
# _project — phase-change detection
# ---------------------------------------------------------------------------


class TestProject:
    def test_new_uid_emits_event(self, projector: EventProjector) -> None:
        obj = _make_obj(uid="uid-1", phase="Detected")
        event = projector._project("incidents", obj)
        assert isinstance(event, IncidentDetected)

    def test_same_phase_returns_none(self, projector: EventProjector) -> None:
        obj = _make_obj(uid="uid-1", phase="Detected")
        projector._project("incidents", obj)  # first call registers phase
        event = projector._project("incidents", obj)  # same phase
        assert event is None

    def test_phase_change_emits_new_event(self, projector: EventProjector) -> None:
        obj = _make_obj(uid="uid-1", phase="Detected")
        projector._project("incidents", obj)
        obj["status"]["phase"] = "Resolved"
        event = projector._project("incidents", obj)
        assert isinstance(event, IncidentResolved)

    def test_unknown_phase_returns_none(self, projector: EventProjector) -> None:
        obj = _make_obj(uid="uid-1", phase="UnknownPhase")
        assert projector._project("incidents", obj) is None

    def test_remediation_planned_maps_from_incident_phase(self, projector: EventProjector) -> None:
        obj = _make_obj(uid="uid-1", phase="RemediationPlanned")
        event = projector._project("incidents", obj)
        assert isinstance(event, RemediationPlanned)

    def test_event_carries_correct_incident_name(self, projector: EventProjector) -> None:
        obj = _make_obj(uid="uid-1", name="my-incident", phase="Detected")
        event = projector._project("incidents", obj)
        assert event is not None
        assert event.incident_name == "my-incident"

    def test_event_carries_correct_namespace(self, projector: EventProjector) -> None:
        obj = _make_obj(uid="uid-1", namespace="prod", phase="Detected")
        event = projector._project("incidents", obj)
        assert event is not None
        assert event.namespace == "prod"

    def test_payload_contains_resource_name_and_phase(self, projector: EventProjector) -> None:
        obj = _make_obj(uid="uid-1", name="my-incident", phase="Detected")
        event = projector._project("incidents", obj)
        assert event is not None
        assert event.payload["resourceName"] == "my-incident"
        assert event.payload["phase"] == "Detected"

    def test_incident_payload_is_enriched(self, projector: EventProjector) -> None:
        obj = _make_obj(
            uid="uid-1",
            name="inc-001",
            phase="Detected",
            spec={
                "summary": "CPU high",
                "severity": "critical",
                "categories": ["resource-saturation"],
                "targetRef": {"kind": "Deployment", "namespace": "prod", "name": "api"},
            },
        )
        event = projector._project("incidents", obj)
        assert event is not None
        assert event.payload["summary"] == "CPU high"
        assert event.payload["severity"] == "critical"
        assert event.payload["categories"] == ["resource-saturation"]
        assert event.payload["targetName"] == "api"

    def test_investigation_payload_uses_string_incident_ref(
        self, projector: EventProjector
    ) -> None:
        obj = _make_obj(
            uid="uid-1",
            name="inv-001",
            phase="Completed",
            spec={
                "incidentRef": "inc-001",
                "summary": "CPU high",
                "severity": "critical",
                "categories": ["resource-saturation", "latency"],
            },
            status={
                "phase": "Completed",
                "result": {
                    "hypothesis": "CPU throttling",
                    "confidence": 0.91,
                    "evidence": [{"skill": "prometheus"}],
                    "recommendedActions": [{"type": "scale-up"}],
                },
            },
        )
        event = projector._project("investigations", obj)
        assert isinstance(event, InvestigationCompleted)
        assert event.incident_name == "inc-001"
        assert event.payload["categories"] == ["resource-saturation", "latency"]
        assert event.payload["confidence"] == 0.91
        assert event.payload["hypothesis"] == "CPU throttling"
        assert event.payload["proposedActionCount"] == 1

    def test_invalid_object_shape_is_skipped(self, projector: EventProjector) -> None:
        assert projector._project("incidents", {"metadata": [], "status": {}}) is None

    def test_incident_added_without_status_maps_to_detected(
        self,
        projector: EventProjector,
    ) -> None:
        obj = {
            "metadata": {
                "uid": "uid-1",
                "name": "inc-001",
                "namespace": "default",
                "resourceVersion": "1",
            },
            "spec": {
                "summary": "CPU high",
                "severity": "critical",
                "categories": ["resource-saturation"],
            },
        }
        event = projector._project("incidents", obj, event_type="ADDED")
        assert isinstance(event, IncidentDetected)
        assert event.payload["phase"] == "Detected"


# ---------------------------------------------------------------------------
# _map_event — full mapping table
# ---------------------------------------------------------------------------


class TestMapEvent:
    BASE: ClassVar[dict[str, object]] = {
        "incidentName": "inc-001",
        "namespace": "default",
        "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
        "payload": {},
    }

    @pytest.mark.parametrize(
        ("plural", "phase", "expected_cls"),
        [
            ("incidents", "Detected", IncidentDetected),
            ("incidents", "RemediationPlanned", RemediationPlanned),
            ("incidents", "Failed", IncidentFailed),
            ("incidents", "Resolved", IncidentResolved),
            ("incidents", "Escalated", EscalationTriggered),
            ("investigations", "InProgress", InvestigationStarted),
            ("investigations", "Completed", InvestigationCompleted),
            ("approvalrequests", "Pending", ApprovalRequired),
            ("approvalrequests", "Rejected", ApprovalRejected),
            ("approvalrequests", "TimedOut", ApprovalTimedOut),
            ("actionexecutions", "Executing", ActionExecuted),
            ("actionexecutions", "Succeeded", ActionSucceeded),
            ("actionexecutions", "Failed", ActionFailed),
            ("actionexecutions", "RolledBack", ActionFailed),
        ],
    )
    def test_known_mapping(
        self, projector: EventProjector, plural: str, phase: str, expected_cls
    ) -> None:
        event = projector._map_event(plural, phase, self.BASE)
        assert isinstance(event, expected_cls)

    def test_unknown_plural_returns_none(self, projector: EventProjector) -> None:
        assert projector._map_event("unknown", "Detected", self.BASE) is None

    def test_unknown_phase_returns_none(self, projector: EventProjector) -> None:
        assert projector._map_event("incidents", "Banana", self.BASE) is None


# ---------------------------------------------------------------------------
# _resolve_incident_name
# ---------------------------------------------------------------------------


class TestResolveIncidentName:
    def test_incident_resource_uses_own_name(self, projector: EventProjector) -> None:
        obj = _make_obj(name="my-incident")
        assert projector._resolve_incident_name("incidents", obj) == "my-incident"

    def test_child_resource_uses_spec_incident_ref(self, projector: EventProjector) -> None:
        obj = _make_obj(incident_name_in_spec="parent-incident")
        assert projector._resolve_incident_name("approvalrequests", obj) == "parent-incident"

    def test_child_resource_falls_back_to_owner_ref(self, projector: EventProjector) -> None:
        obj = _make_obj(owner_incident="owner-incident")
        assert projector._resolve_incident_name("investigations", obj) == "owner-incident"

    def test_dict_incident_ref_is_not_accepted(self, projector: EventProjector) -> None:
        obj = _make_obj(spec={"incidentRef": {"name": "legacy-shape"}})
        assert projector._resolve_incident_name("investigations", obj) == "unknown"

    def test_child_resource_with_no_ref_returns_unknown(self, projector: EventProjector) -> None:
        obj = _make_obj()
        assert projector._resolve_incident_name("investigations", obj) == "unknown"


class TestWatchResource:
    @pytest.mark.asyncio
    async def test_priming_skips_initial_snapshot_replay(
        self, projector: EventProjector, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        existing = _make_obj(uid="uid-1", phase="Detected")

        async def fake_list_namespaced_custom_object(**_kwargs):
            return {"items": [existing], "metadata": {"resourceVersion": "10"}}

        class FakeWatch:
            def stream(self, *_args, **_kwargs):
                async def gen():
                    yield {"type": "ADDED", "object": existing}
                    yield {
                        "type": "MODIFIED",
                        "object": _make_obj(uid="uid-1", phase="Resolved"),
                    }

                return gen()

        monkeypatch.setattr("kubortex.edge.core.projector.watch.Watch", FakeWatch)

        api = SimpleNamespace(list_namespaced_custom_object=fake_list_namespaced_custom_object)
        resource_version = await projector._initialize_resource_watch_state(
            api,
            projector._settings,
            "incidents",
        )
        event = await asyncio.wait_for(
            anext(
                projector._watch_resource(api, projector._settings, "incidents", resource_version)
            ),
            timeout=1,
        )

        assert isinstance(event, IncidentResolved)

    @pytest.mark.asyncio
    async def test_deleted_event_prunes_seen_phase_cache(
        self, projector: EventProjector, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        projector._seen_phases["ignored"] = "Detected"

        class FakeWatch:
            def stream(self, *_args, **_kwargs):
                async def gen():
                    yield {"type": "DELETED", "object": _make_obj(uid="ignored", phase="Detected")}
                    yield {"type": "ADDED", "object": _make_obj(uid="uid-1", phase="Detected")}

                return gen()

        monkeypatch.setattr("kubortex.edge.core.projector.watch.Watch", FakeWatch)

        api = SimpleNamespace(list_namespaced_custom_object=object())
        event = await asyncio.wait_for(
            anext(projector._watch_resource(api, projector._settings, "incidents")),
            timeout=1,
        )

        assert isinstance(event, IncidentDetected)
        assert "ignored" not in projector._seen_phases

    @pytest.mark.asyncio
    async def test_retries_after_watch_error(
        self, projector: EventProjector, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        sleep = AsyncMock()

        async def fake_list_namespaced_custom_object(**_kwargs):
            return {"items": [], "metadata": {"resourceVersion": "10"}}

        class FakeWatch:
            calls = 0

            def stream(self, *_args, **_kwargs):
                async def gen():
                    type(self).calls += 1
                    if type(self).calls == 1:
                        raise RuntimeError("boom")
                    yield {"type": "ADDED", "object": _make_obj(uid="uid-1", phase="Detected")}

                return gen()

        monkeypatch.setattr("kubortex.edge.core.projector.watch.Watch", FakeWatch)
        monkeypatch.setattr("kubortex.edge.core.projector.asyncio.sleep", sleep)

        api = SimpleNamespace(list_namespaced_custom_object=fake_list_namespaced_custom_object)
        event = await asyncio.wait_for(
            anext(projector._watch_resource(api, projector._settings, "incidents")),
            timeout=1,
        )

        assert isinstance(event, IncidentDetected)
        sleep.assert_awaited_once_with(5)


class TestWatchEvents:
    @pytest.mark.asyncio
    async def test_yields_events_from_concurrent_resource_watchers(
        self, projector: EventProjector, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def fake_prime(_api, _settings, _plural: str):
            return "10"

        async def fake_watch_resource(_api, _settings, plural: str, _resource_version: str | None):
            if plural == "incidents":
                yield IncidentDetected(
                    incidentName="inc-001",
                    namespace="default",
                    timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                    payload={"phase": "Detected"},
                )
                return
            return

        monkeypatch.setattr(
            "kubortex.edge.core.projector.get_kubernetes_clients",
            AsyncMock(return_value=SimpleNamespace(custom_objects=object())),
        )
        monkeypatch.setattr(projector, "_initialize_resource_watch_state", fake_prime)
        monkeypatch.setattr(projector, "_watch_resource", fake_watch_resource)

        stream = projector.watch_events()
        event = await asyncio.wait_for(anext(stream), timeout=1)
        await stream.aclose()

        assert isinstance(event, IncidentDetected)
        assert event.incident_name == "inc-001"
        assert projector.is_ready is False
