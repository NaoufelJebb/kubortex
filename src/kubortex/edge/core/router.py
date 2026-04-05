"""Core notification routing workflow for Edge.

Sinks implement the ``NotificationSink`` protocol. Register them at startup
via ``router.register(sink)`` before calling ``router.run()``.  The router
fans out each event to all registered sinks, so adding a new delivery channel
requires no changes to this file.
"""

from __future__ import annotations

import asyncio
from typing import Protocol, runtime_checkable

import structlog

from kubortex.edge.core.events import DomainEvent
from kubortex.shared.config import EdgeSettings

from .projector import EventProjector

logger = structlog.get_logger(__name__)
_SINK_SEND_TIMEOUT_SECONDS = 5.0


@runtime_checkable
class NotificationSink(Protocol):
    """Protocol for async notification sinks."""

    async def send(self, event: DomainEvent) -> None:
        """Deliver a domain event.

        Args:
            event: Event to send.
        """
        ...


class NotificationRouter:
    """Fan out projected notification events to registered sinks."""

    def __init__(self, settings: EdgeSettings | None = None) -> None:
        self._settings = settings or EdgeSettings()
        self._projector = EventProjector(self._settings)
        self._sinks: list[NotificationSink] = []

    @property
    def enabled(self) -> bool:
        """Whether notifications are configured."""
        return bool(self._sinks)

    @property
    def is_ready(self) -> bool:
        """Whether the notification subsystem is ready."""
        return True if not self.enabled else self._projector.is_ready

    def register(self, sink: NotificationSink) -> None:
        """Register a notification sink.

        Args:
            sink: Sink to add.
        """
        self._sinks.append(sink)

    async def run(self) -> None:
        """Consume projected events and dispatch them to all sinks.

        Sink sends run concurrently per event and are bounded by a timeout so
        one slow delivery target does not stall the whole notification path.
        """
        if not self.enabled:
            logger.info("notification_router_disabled")
            return

        logger.info("notification_router_started", sinks=len(self._sinks))

        async for event in self._projector.watch_events():
            try:
                await self._dispatch(event)
            except Exception:
                logger.exception("dispatch_error", event_type=event.event_type)

    async def _dispatch(self, event: DomainEvent) -> None:
        """Send an event to every registered sink.

        Args:
            event: Event to dispatch.

        Sink sends run concurrently and each send is wrapped in a timeout so
        one slow delivery target cannot block sibling sinks forever.
        """
        async def _send(sink: NotificationSink) -> Exception | None:
            try:
                await asyncio.wait_for(
                    sink.send(event),
                    timeout=_SINK_SEND_TIMEOUT_SECONDS,
                )
            except Exception as exc:
                return exc
            return None

        results = await asyncio.gather(*(_send(sink) for sink in self._sinks))

        for sink, result in zip(self._sinks, results, strict=False):
            if result is None:
                continue
            if isinstance(result, asyncio.TimeoutError):
                logger.error(
                    "sink_error",
                    sink=type(sink).__name__,
                    event_type=event.event_type,
                    reason="timeout",
                    error=str(result),
                )
                continue
            logger.error(
                "sink_error",
                sink=type(sink).__name__,
                event_type=event.event_type,
                error=str(result),
            )
