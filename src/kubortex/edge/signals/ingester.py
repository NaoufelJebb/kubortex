"""Signal ingestion — pluggable source protocol and HTTP dispatcher.

Sources implement ``SignalSource`` and are registered via
``SignalIngester.register()``.  The ingester creates a FastAPI route per
source and handles the generic grouping + correlation logic, so individual
sources only need to parse their own payload format.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

import structlog
from fastapi import APIRouter, Request, Response

from kubortex.shared.config import EdgeSettings
from kubortex.shared.models.incident import Signal, TargetRef
from kubortex.shared.types import Category

from .correlator import correlate_and_upsert

logger = structlog.get_logger(__name__)


@runtime_checkable
class SignalSource(Protocol):
    """Protocol for webhook-backed signal sources."""

    path: str  # HTTP path for this source's webhook, e.g. "/api/v1/alerts"

    async def parse(
        self, payload: dict[str, Any]
    ) -> list[tuple[Signal, Category, TargetRef | None]]:
        """Parse a webhook payload into signal tuples.

        Args:
            payload: Raw webhook body.

        Returns:
            Parsed ``(signal, category, target)`` tuples.
        """
        ...


class SignalIngester:
    """Register signal sources and forward parsed signals to correlation."""

    def __init__(self, settings: EdgeSettings | None = None) -> None:
        self._settings = settings or EdgeSettings()
        self._router = APIRouter(tags=["signals"])
        self._sources: list[SignalSource] = []

    @property
    def router(self) -> APIRouter:
        """Return the FastAPI router for registered sources.

        Returns:
            Router exposing source webhook endpoints.
        """
        return self._router

    def register(self, source: SignalSource) -> None:
        """Register a source and add its webhook endpoint.

        Args:
            source: Source to expose through FastAPI.
        """
        self._sources.append(source)
        self._router.add_api_route(
            source.path,
            self._make_handler(source),
            methods=["POST"],
        )

    def _make_handler(self, source: SignalSource):
        namespace = self._settings.namespace
        crd_group = self._settings.crd_group
        crd_version = self._settings.crd_version
        correlation_window_seconds = self._settings.correlation_window_seconds

        async def _handler(request: Request) -> Response:
            body: dict[str, Any] = await request.json()
            parsed = await source.parse(body)

            if not parsed:
                return Response(status_code=200, content="no signals")

            groups: dict[str, tuple[list[Signal], Category, TargetRef | None]] = {}
            for signal, category, target in parsed:
                key = (
                    f"{category}:{target.namespace}/{target.name}"
                    if target
                    else f"{category}:unknown"
                )
                if key not in groups:
                    groups[key] = ([], category, target)
                groups[key][0].append(signal)

            created: list[str] = []
            for _key, (signals, category, target) in groups.items():
                inc_name = await correlate_and_upsert(
                    signals,
                    category,
                    target,
                    namespace,
                    crd_group,
                    crd_version,
                    correlation_window_seconds,
                )
                created.append(inc_name)

            logger.info(
                "signals_ingested",
                source=type(source).__name__,
                signal_count=len(parsed),
                incident_count=len(created),
            )
            return Response(status_code=200, content=f"processed {len(parsed)} signals")

        return _handler
