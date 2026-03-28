"""Unit tests for kubortex.edge.notifications.router."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kubortex.edge.notifications.events import IncidentDetected, InvestigationStarted
from kubortex.edge.notifications.router import NotificationRouter, NotificationSink
from kubortex.edge.notifications.slack import SlackNotifier
from kubortex.shared.config import EdgeSettings

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_NOW = datetime(2024, 1, 1, tzinfo=UTC)

_BASE_KWARGS = {"incidentName": "inc-001", "namespace": "default", "timestamp": _NOW, "payload": {}}


def _event(cls=IncidentDetected, **extra):
    kwargs = dict(_BASE_KWARGS)
    kwargs["payload"] = extra
    return cls(**kwargs)


class _FakeSink:
    """Minimal NotificationSink implementation for testing."""

    def __init__(self) -> None:
        self.received: list = []

    async def send(self, event) -> None:
        self.received.append(event)


# ---------------------------------------------------------------------------
# NotificationSink Protocol
# ---------------------------------------------------------------------------


class TestNotificationSinkProtocol:
    def test_fake_sink_satisfies_protocol(self) -> None:
        assert isinstance(_FakeSink(), NotificationSink)

    def test_slack_notifier_satisfies_protocol(self) -> None:
        settings = EdgeSettings(slack_bot_token="xoxb-test")
        assert isinstance(SlackNotifier(settings), NotificationSink)

    def test_object_without_send_does_not_satisfy_protocol(self) -> None:
        assert not isinstance(object(), NotificationSink)


# ---------------------------------------------------------------------------
# NotificationRouter.register
# ---------------------------------------------------------------------------


class TestRegister:
    def test_register_adds_sink(self) -> None:
        router = NotificationRouter.__new__(NotificationRouter)
        router._sinks = []
        sink = _FakeSink()
        router.register(sink)
        assert sink in router._sinks

    def test_register_multiple_sinks(self) -> None:
        router = NotificationRouter.__new__(NotificationRouter)
        router._sinks = []
        s1, s2 = _FakeSink(), _FakeSink()
        router.register(s1)
        router.register(s2)
        assert router._sinks == [s1, s2]


# ---------------------------------------------------------------------------
# NotificationRouter._dispatch
# ---------------------------------------------------------------------------


class TestDispatch:
    @pytest.mark.asyncio
    async def test_dispatches_to_all_sinks(self) -> None:
        router = NotificationRouter.__new__(NotificationRouter)
        s1, s2 = _FakeSink(), _FakeSink()
        router._sinks = [s1, s2]
        event = _event()
        await router._dispatch(event)
        assert s1.received == [event]
        assert s2.received == [event]

    @pytest.mark.asyncio
    async def test_sink_error_does_not_stop_other_sinks(self) -> None:
        """A failing sink must not prevent subsequent sinks from receiving the event."""

        class _BrokenSink:
            async def send(self, event) -> None:
                raise RuntimeError("broken")

        router = NotificationRouter.__new__(NotificationRouter)
        good = _FakeSink()
        router._sinks = [_BrokenSink(), good]
        await router._dispatch(_event())
        assert len(good.received) == 1

    @pytest.mark.asyncio
    async def test_no_sinks_does_not_raise(self) -> None:
        router = NotificationRouter.__new__(NotificationRouter)
        router._sinks = []
        await router._dispatch(_event())  # must not raise


# ---------------------------------------------------------------------------
# NotificationRouter.run
# ---------------------------------------------------------------------------


class TestInit:
    def test_constructor_sets_empty_sinks_list(self) -> None:
        settings = EdgeSettings()
        router = NotificationRouter(settings)
        assert router._sinks == []

    def test_constructor_uses_default_settings_when_none(self) -> None:
        router = NotificationRouter()
        assert router._settings is not None
        assert router._projector is not None


class TestRun:
    @pytest.mark.asyncio
    async def test_run_dispatches_events_from_projector(self) -> None:
        events = [_event(IncidentDetected), _event(InvestigationStarted)]

        async def _fake_watch():
            for e in events:
                yield e

        router = NotificationRouter.__new__(NotificationRouter)
        router._projector = MagicMock()
        router._projector.watch_events = _fake_watch
        sink = _FakeSink()
        router._sinks = [sink]
        await router.run()
        assert sink.received == events

    @pytest.mark.asyncio
    async def test_run_logs_sink_count(self, caplog) -> None:
        async def _empty_watch():
            return
            yield  # make it an async generator

        import structlog.testing

        router = NotificationRouter.__new__(NotificationRouter)
        router._projector = MagicMock()
        router._projector.watch_events = _empty_watch
        router._sinks = [_FakeSink(), _FakeSink()]

        with structlog.testing.capture_logs() as logs:
            await router.run()

        start_log = next(
            (l for l in logs if l.get("event") == "notification_router_started"), None
        )
        assert start_log is not None
        assert start_log["sinks"] == 2

    @pytest.mark.asyncio
    async def test_run_swallows_dispatch_exception_and_continues(self) -> None:
        """Exceptions from _dispatch must not abort the event loop."""
        import structlog.testing

        events = [_event(IncidentDetected), _event(InvestigationStarted)]
        call_count = 0

        async def _fake_watch():
            for e in events:
                yield e

        router = NotificationRouter.__new__(NotificationRouter)
        router._projector = MagicMock()
        router._projector.watch_events = _fake_watch
        router._sinks = []

        original_dispatch = router._dispatch.__func__

        async def _raising_dispatch(self, event):
            nonlocal call_count
            call_count += 1
            raise RuntimeError("unexpected dispatch failure")

        router._dispatch = _raising_dispatch.__get__(router)

        with structlog.testing.capture_logs() as logs:
            await router.run()

        assert call_count == 2  # both events were attempted
        error_logs = [l for l in logs if l.get("event") == "dispatch_error"]
        assert len(error_logs) == 2
