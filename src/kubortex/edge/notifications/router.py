"""Notification router — dispatches domain events to configured sinks.

For MVP, the only sink is Slack.  The router consumes events from the
projector and fans them out to all registered sinks.
"""

from __future__ import annotations

import structlog

from kubortex.shared.config import KubortexSettings

from .events import DomainEvent
from .projector import EventProjector
from .slack import SlackNotifier

logger = structlog.get_logger(__name__)


class NotificationRouter:
    """Consumes domain events from the projector and delivers to sinks."""

    def __init__(self, settings: KubortexSettings | None = None) -> None:
        self._settings = settings or KubortexSettings()
        self._projector = EventProjector(self._settings)
        self._slack = SlackNotifier(self._settings)

    async def run(self) -> None:
        """Start the event loop: watch CRDs → project events → deliver."""
        logger.info("notification_router_started")

        async for event in self._projector.watch_events():
            try:
                await self._dispatch(event)
            except Exception:
                logger.exception("dispatch_error", event=event.event_type)

    async def _dispatch(self, event: DomainEvent) -> None:
        """Send a domain event to all configured sinks."""
        await self._slack.send(event)
