"""Alertmanager signal source — parses Alertmanager webhook payloads."""

from __future__ import annotations

from typing import Any

from kubortex.shared.models.incident import Signal, TargetRef
from kubortex.shared.types import Category

from .normaliser import normalise_alert


class AlertmanagerSource:
    """Parse Alertmanager webhook payloads."""

    path: str = "/api/v1/alerts"

    async def parse(
        self, payload: dict[str, Any]
    ) -> list[tuple[Signal, Category, TargetRef | None]]:
        """Extract firing alerts and normalize them into signal tuples.

        Args:
            payload: Alertmanager webhook payload.

        Returns:
            Parsed ``(signal, category, target)`` tuples for firing alerts.
        """
        return [
            normalise_alert(alert)
            for alert in payload.get("alerts", [])
            if alert.get("status") != "resolved"
        ]
