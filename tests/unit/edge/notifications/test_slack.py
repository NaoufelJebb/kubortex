"""Unit tests for kubortex.edge.notifications.slack.SlackNotifier."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from kubortex.edge.core.events import (
    ActionFailed,
    ApprovalRejected,
    ApprovalTimedOut,
    EscalationTriggered,
    IncidentDetected,
    IncidentFailed,
    IncidentResolved,
    InvestigationStarted,
)
from kubortex.edge.notifications import slack as slack_module
from kubortex.edge.notifications.slack import SafeFormatDict, SlackNotifier
from kubortex.shared.config import EdgeSettings

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


_NOW = datetime(2024, 1, 1, tzinfo=UTC)

_BASE_EVENT_KWARGS = {
    "incidentName": "inc-001",
    "namespace": "default",
    "timestamp": _NOW,
    "payload": {},
}


def _make_event(cls, **payload_extra):
    kwargs = dict(_BASE_EVENT_KWARGS)
    kwargs["payload"] = dict(payload_extra)
    return cls(**kwargs)


@pytest.fixture()
def settings_with_token() -> EdgeSettings:
    return EdgeSettings(
        slack_bot_token="xoxb-test-token",
        slack_channel="#oncall",
        slack_escalation_channel="#escalations",
    )


@pytest.fixture()
def settings_no_token() -> EdgeSettings:
    return EdgeSettings(slack_bot_token="")


@pytest.fixture()
def mock_slack_client() -> AsyncMock:
    client = AsyncMock()
    client.chat_postMessage = AsyncMock(return_value={"ok": True, "ts": "111.222"})
    return client


@pytest.fixture()
def notifier(settings_with_token, mock_slack_client, monkeypatch) -> SlackNotifier:
    n = SlackNotifier(settings_with_token)
    n._client = mock_slack_client
    return n


# ---------------------------------------------------------------------------
# SafeFormatDict
# ---------------------------------------------------------------------------


class TestSafeFormatDict:
    def test_missing_key_returns_placeholder(self) -> None:
        d = SafeFormatDict({"a": "1"})
        assert d["missing"] == "<missing>"

    def test_present_key_returns_value(self) -> None:
        d = SafeFormatDict({"key": "val"})
        assert d["key"] == "val"


# ---------------------------------------------------------------------------
# SlackNotifier._render
# ---------------------------------------------------------------------------


class TestRender:
    @pytest.fixture()
    def n(self, settings_with_token: EdgeSettings) -> SlackNotifier:
        return SlackNotifier(settings_with_token)

    def test_known_event_type_renders_emoji_prefix(self, n: SlackNotifier) -> None:
        event = _make_event(
            IncidentDetected,
            summary="CPU high",
            severity="critical",
            category="resource-saturation",
        )
        text = n._render(event)
        assert text.startswith(":rotating_light:")

    def test_unknown_event_type_falls_back_to_generic(self, n: SlackNotifier) -> None:
        from kubortex.edge.core.events import DomainEvent

        event = DomainEvent(
            eventType="MyCustomEvent", incidentName="inc-001", namespace="default", timestamp=_NOW
        )
        text = n._render(event)
        assert "[MyCustomEvent]" in text
        assert "inc-001" in text

    def test_missing_format_placeholder_does_not_raise(self, n: SlackNotifier) -> None:
        # IncidentDetected template expects summary/severity/category.
        # With an empty payload, Slack should fall back to the incident name and
        # generic labels instead of rendering placeholders.
        event = _make_event(IncidentDetected)
        text = n._render(event)
        assert text == (
            ":rotating_light: Incident detected: *inc-001*\nSeverity: unknown | Category: unknown"
        )

    def test_incident_resolved_uses_incident_name(self, n: SlackNotifier) -> None:
        event = _make_event(IncidentResolved)
        text = n._render(event)
        assert "inc-001" in text

    def test_action_failed_renders_resource_name(self, n: SlackNotifier) -> None:
        event = _make_event(ActionFailed, actionType="restart-pod", targetName="api")
        text = n._render(event)
        assert text == ":x: Action failed for `restart-pod` on `api`"

    def test_approval_rejected_renders_action_context(self, n: SlackNotifier) -> None:
        event = _make_event(ApprovalRejected, actionType="restart-pod", targetName="api")
        text = n._render(event)
        assert text == ":no_entry_sign: Approval rejected for `restart-pod` on `api`"

    def test_approval_timed_out_renders_action_context(self, n: SlackNotifier) -> None:
        event = _make_event(ApprovalTimedOut, actionType="restart-pod", targetName="api")
        text = n._render(event)
        assert text == ":alarm_clock: Approval timed out for `restart-pod` on `api`"

    def test_incident_failed_uses_summary_fallback(self, n: SlackNotifier) -> None:
        event = _make_event(IncidentFailed)
        text = n._render(event)
        assert text == ":warning: Incident retry triggered: *inc-001*"

    def test_render_context_applies_fallbacks(self, n: SlackNotifier) -> None:
        context = n._build_render_context(_make_event(IncidentDetected))
        assert context["summary"] == "inc-001"
        assert context["severity"] == "unknown"

    def test_malformed_template_falls_back_to_generic_message(
        self, n: SlackNotifier, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setitem(
            slack_module._TEMPLATES,
            "IncidentDetected",
            (":rotating_light:", "Incident detected: {"),
        )

        text = n._render(_make_event(IncidentDetected))

        assert text == ":rotating_light: IncidentDetected: inc-001"


# ---------------------------------------------------------------------------
# SlackNotifier.send
# ---------------------------------------------------------------------------


class TestSend:
    @pytest.mark.asyncio
    async def test_no_token_skips_sending(
        self, settings_no_token: EdgeSettings, mock_slack_client: AsyncMock
    ) -> None:
        n = SlackNotifier(settings_no_token)
        n._client = mock_slack_client
        await n.send(_make_event(IncidentDetected))
        mock_slack_client.chat_postMessage.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_sends_to_default_channel(
        self, notifier: SlackNotifier, mock_slack_client: AsyncMock
    ) -> None:
        await notifier.send(_make_event(InvestigationStarted))
        call_kwargs = mock_slack_client.chat_postMessage.await_args.kwargs
        assert call_kwargs["channel"] == "#oncall"
        assert call_kwargs["thread_ts"] is None
        assert call_kwargs["text"] == ":mag: Investigation started for *inc-001*"

    @pytest.mark.asyncio
    async def test_escalation_sends_to_escalation_channel(
        self, notifier: SlackNotifier, mock_slack_client: AsyncMock
    ) -> None:
        await notifier.send(_make_event(EscalationTriggered))
        call_kwargs = mock_slack_client.chat_postMessage.await_args.kwargs
        assert call_kwargs["channel"] == "#escalations"

    @pytest.mark.asyncio
    async def test_incident_detected_stores_thread_ts(
        self, notifier: SlackNotifier, mock_slack_client: AsyncMock
    ) -> None:
        mock_slack_client.chat_postMessage.return_value = {"ok": True, "ts": "999.000"}
        await notifier.send(_make_event(IncidentDetected))
        assert notifier._threads.get(("#oncall", "inc-001")) == "999.000"

    @pytest.mark.asyncio
    async def test_follow_up_event_uses_thread_ts(
        self, notifier: SlackNotifier, mock_slack_client: AsyncMock
    ) -> None:
        # Seed the thread
        notifier._threads[("#oncall", "inc-001")] = "999.000"
        await notifier.send(_make_event(InvestigationStarted))
        call_kwargs = mock_slack_client.chat_postMessage.await_args.kwargs
        assert call_kwargs["thread_ts"] == "999.000"

    @pytest.mark.asyncio
    async def test_escalation_does_not_reuse_main_channel_thread(
        self, notifier: SlackNotifier, mock_slack_client: AsyncMock
    ) -> None:
        notifier._threads[("#oncall", "inc-001")] = "999.000"

        await notifier.send(_make_event(EscalationTriggered))

        call_kwargs = mock_slack_client.chat_postMessage.await_args.kwargs
        assert call_kwargs["channel"] == "#escalations"
        assert call_kwargs["thread_ts"] is None

    @pytest.mark.asyncio
    async def test_slack_error_is_swallowed(
        self, notifier: SlackNotifier, mock_slack_client: AsyncMock
    ) -> None:
        mock_slack_client.chat_postMessage.side_effect = Exception("Slack is down")
        # Should not raise
        await notifier.send(_make_event(IncidentDetected))

    @pytest.mark.asyncio
    async def test_failed_response_does_not_store_thread(
        self, notifier: SlackNotifier, mock_slack_client: AsyncMock
    ) -> None:
        mock_slack_client.chat_postMessage.return_value = {"ok": False}
        await notifier.send(_make_event(IncidentDetected))
        assert ("#oncall", "inc-001") not in notifier._threads

    @pytest.mark.asyncio
    async def test_non_detection_event_does_not_create_thread(
        self, notifier: SlackNotifier, mock_slack_client: AsyncMock
    ) -> None:
        mock_slack_client.chat_postMessage.return_value = {"ok": True, "ts": "999.000"}

        await notifier.send(_make_event(InvestigationStarted))

        assert notifier._threads == {}

    @pytest.mark.asyncio
    async def test_ok_response_without_ts_does_not_store_thread(
        self, notifier: SlackNotifier, mock_slack_client: AsyncMock
    ) -> None:
        mock_slack_client.chat_postMessage.return_value = {"ok": True, "ts": ""}
        await notifier.send(_make_event(IncidentDetected))
        assert ("#oncall", "inc-001") not in notifier._threads
