"""Core signal ingestion workflow for Edge.

Sources implement ``SignalSource`` and are registered via
``SignalIngester.register()``.  The ingester creates a FastAPI route per
source and handles the generic grouping + correlation logic, so individual
sources only need to parse their own payload format.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

import structlog
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from kubortex.shared.config import EdgeSettings
from kubortex.shared.models.incident import Signal, TargetRef
from kubortex.shared.types import Category

from .correlator import _correlation_key, correlate_and_upsert

logger = structlog.get_logger(__name__)


@runtime_checkable
class SignalSource(Protocol):
    """Protocol for webhook-backed signal sources."""

    path: str  # HTTP path for this source's webhook, e.g. "/api/v1/alerts"
    source_name: str  # Identifier stamped on incidents, e.g. "alertmanager"

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
        """Build the FastAPI request handler for a registered signal source.

        Closes over the current settings so the returned handler can be
        registered as a standalone callable with FastAPI.

        Args:
            source: The signal source whose webhook this handler serves.

        Returns:
            An async callable suitable for ``APIRouter.add_api_route``.
        """
        settings = self._settings

        async def _handler(request: Request) -> JSONResponse:
            try:
                body = await request.json()
            except ValueError as exc:
                raise HTTPException(
                    status_code=400, detail="request body must be valid JSON"
                ) from exc

            if not isinstance(body, dict):
                raise HTTPException(status_code=400, detail="request body must be a JSON object")

            try:
                parsed = await source.parse(body)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc

            if not parsed:
                return JSONResponse(status_code=200, content={"accepted": 0})

            # Group by target identity only — category is not part of the key.
            # Signals from different categories targeting the same workload are
            # batched together so they land in a single Incident.
            groups: dict[str, tuple[list[Signal], list[Category], TargetRef | None]] = {}
            for signal, category, target in parsed:
                key = _correlation_key(target)
                if key not in groups:
                    groups[key] = ([], [], target)
                groups[key][0].append(signal)
                if category not in groups[key][1]:
                    groups[key][1].append(category)

            created: list[str] = []
            for signals, categories, target in groups.values():
                inc_name = await correlate_and_upsert(
                    signals,
                    categories,
                    target,
                    settings.namespace,
                    settings.crd_group,
                    settings.crd_version,
                    settings.correlation_window_seconds,
                    source=source.source_name,
                    max_signals=settings.max_signals_per_incident,
                )
                created.append(inc_name)

            logger.info(
                "signals_ingested",
                source=type(source).__name__,
                signal_count=len(parsed),
                incident_count=len(created),
            )
            return JSONResponse(
                status_code=200,
                content={"accepted": len(parsed), "incidents": len(created)},
            )

        return _handler
