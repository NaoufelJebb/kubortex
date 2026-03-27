"""Slack notification sink — renders domain events as Slack messages.

Uses the Slack Web API (slack_sdk) to post messages.  Messages for the
same incident are threaded under the initial detection message.
"""

from __future__ import annotations

from typing import Any

import structlog
from slack_sdk.web.async_client import AsyncWebClient

from kubortex.shared.config import EdgeSettings

from .events import DomainEvent

logger = structlog.get_logger(__name__)

# Map event types to emoji + summary template
_TEMPLATES: dict[str, tuple[str, str]] = {
    "IncidentDetected": (
        ":rotating_light:",
        "Incident detected: *{summary}*\nSeverity: {severity} | Category: {category}",
    ),
    "InvestigationStarted": (":mag:", "Investigation started for `{resourceName}`"),
    "InvestigationCompleted": (
        ":white_check_mark:",
        "Investigation completed — confidence: {confidence}",
    ),
    "RemediationPlanned": (":clipboard:", "Remediation plan created: `{resourceName}`"),
    "ApprovalRequired": (":raised_hand:", "Approval required for action: `{resourceName}`"),
    "ActionExecuted": (":gear:", "Executing action: `{resourceName}`"),
    "ActionSucceeded": (":tada:", "Action succeeded: `{resourceName}`"),
    "ActionFailed": (":x:", "Action failed: `{resourceName}`"),
    "IncidentResolved": (":green_circle:", "Incident resolved: `{incidentName}`"),
    "EscalationTriggered": (":fire:", "Escalation triggered — human review needed"),
}


class SlackNotifier:
    """Sends domain events to Slack threads by incident."""

    def __init__(self, settings: EdgeSettings | None = None) -> None:
        self._settings = settings or EdgeSettings()
        self._client = AsyncWebClient(token=self._settings.slack_bot_token)
        self._channel = self._settings.slack_channel
        self._escalation_channel = self._settings.slack_escalation_channel
        # incident_name -> thread_ts for threading
        self._threads: dict[str, str] = {}

    async def send(self, event: DomainEvent) -> None:
        """Render and send a domain event.

        Args:
            event: Event to deliver.
        """
        if not self._settings.slack_bot_token:
            logger.debug("slack_disabled", reason="no bot token")
            return

        text = self._render(event)
        channel = self._channel

        # Escalations go to a separate channel
        if event.event_type == "EscalationTriggered":
            channel = self._escalation_channel

        thread_ts = self._threads.get(event.incident_name)

        try:
            response = await self._client.chat_postMessage(
                channel=channel,
                text=text,
                thread_ts=thread_ts,
            )

            # Track thread for this incident
            if event.event_type == "IncidentDetected" and response.get("ok"):
                ts = response.get("ts", "")
                if ts:
                    self._threads[event.incident_name] = ts

            logger.debug(
                "slack_message_sent",
                event_type=event.event_type,
                channel=channel,
            )

        except Exception:
            logger.exception("slack_send_failed", event_type=event.event_type)

    def _render(self, event: DomainEvent) -> str:
        """Render a Slack message for a domain event.

        Args:
            event: Event to render.

        Returns:
            Rendered Slack message text.
        """
        template = _TEMPLATES.get(event.event_type)
        if not template:
            return f"[{event.event_type}] {event.incident_name}"

        emoji, msg_template = template

        # Build format kwargs from event payload + top-level fields
        fmt_kwargs: dict[str, Any] = {
            "incidentName": event.incident_name,
            "namespace": event.namespace,
            **event.payload,
        }

        try:
            msg = msg_template.format_map(SafeFormatDict(fmt_kwargs))
        except Exception:
            msg = f"{event.event_type}: {event.incident_name}"

        return f"{emoji} {msg}"


class SafeFormatDict(dict[str, Any]):
    """Returns placeholder names for missing format keys."""

    def __missing__(self, key: str) -> str:
        return f"<{key}>"
