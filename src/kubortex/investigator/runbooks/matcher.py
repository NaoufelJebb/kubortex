"""Runbook matcher — select the best runbook for an investigation.

Matching uses metadata.match rules (categories, severities) and
metadata.priority to break ties. Highest priority wins.
"""

from __future__ import annotations

import structlog

from .models import RunbookManifest
from .registry import RunbookRegistry

logger = structlog.get_logger(__name__)


def match_runbook(
    registry: RunbookRegistry,
    category: str,
    severity: str,
) -> RunbookManifest | None:
    """Find the highest-priority matching runbook for the given incident context.

    Returns the best match, or None if no runbook matches.
    """
    candidates: list[tuple[int, RunbookManifest]] = []

    for manifest in registry.all:
        match_rules = manifest.metadata.get("match", {})
        categories = match_rules.get("categories", [])
        severities = match_rules.get("severities", [])
        priority = match_rules.get("priority", 0)

        # Category must match if specified
        if categories and category not in categories:
            continue

        # Severity must match if specified
        if severities and severity not in severities:
            continue

        candidates.append((priority, manifest))

    if not candidates:
        logger.debug("no_runbook_match", category=category, severity=severity)
        return None

    # Highest priority wins
    candidates.sort(key=lambda x: x[0], reverse=True)
    winner = candidates[0][1]
    logger.info("runbook_matched", name=winner.name, category=category, severity=severity)
    return winner
