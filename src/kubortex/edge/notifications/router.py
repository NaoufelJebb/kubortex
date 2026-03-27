"""Notification router — dispatches domain events to configured sinks.

Sinks implement the ``NotificationSink`` protocol.  Register them at startup
via ``router.register(sink)`` before calling ``router.run()``.  The router
fans out each event to all registered sinks, so adding a new delivery channel
(PagerDuty, email, webhook) requires no changes to this file.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

import structlog

from kubortex.shared.config import KubortexSettings

from .events import DomainEvent
from .projector import EventProjector

logger = structlog.get_logger(__name__)


@runtime_checkable
class NotificationSink(Protocol):
    """Any object with an async ``send`` method is a valid notification sink."""

    async def send(self, event: DomainEvent) -> None:
        """Deliver a domain event to the sink."""
        ...


class NotificationRouter:
    """Consumes domain events from the projector and fans out to registered sinks."""

    def __init__(self, settings: KubortexSettings | None = None) -> None:
        self._settings = settings or KubortexSettings()
        self._projector = EventProjector(self._settings)
        self._sinks: list[NotificationSink] = []

    def register(self, sink: NotificationSink) -> None:
        """Add a sink.  Call before ``run()``."""
        self._sinks.append(sink)

    async def run(self) -> None:
        """Start the event loop: watch CRDs → project events → deliver to all sinks."""
        logger.info("notification_router_started", sinks=len(self._sinks))

        async for event in self._projector.watch_events():
            try:
                await self._dispatch(event)
            except Exception:
                logger.exception("dispatch_error", event=event.event_type)

    async def _dispatch(self, event: DomainEvent) -> None:
        """Fan out a domain event to every registered sink."""
        for sink in self._sinks:
            try:
                await sink.send(event)
            except Exception:
                logger.exception(
                    "sink_error",
                    sink=type(sink).__name__,
                    event_type=event.event_type,
                )
