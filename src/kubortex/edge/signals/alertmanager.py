"""Alertmanager signal source — parses Alertmanager webhook payloads."""

from __future__ import annotations

from typing import Any

from kubortex.shared.models.incident import Signal, TargetRef
from kubortex.shared.types import Category

from .normaliser import normalise_alert


class AlertmanagerSource:
    """Parse Alertmanager webhook payloads."""

    path: str = "/api/v1/alerts"
    source_name: str = "alertmanager"

    async def parse(
        self, payload: dict[str, Any]
    ) -> list[tuple[Signal, Category, TargetRef | None]]:
        """Extract firing alerts and normalize them into signal tuples.

        Args:
            payload: Alertmanager webhook payload.

        Returns:
            Parsed ``(signal, category, target)`` tuples for firing alerts.
        """
        parsed: list[tuple[Signal, Category, TargetRef | None]] = []
        for alert in payload.get("alerts", []):
            if not isinstance(alert, dict):
                raise ValueError("each alert must be a JSON object")
            if alert.get("status") == "resolved":
                continue
            parsed.append(await normalise_alert(alert))
        return parsed
