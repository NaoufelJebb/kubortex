"""Feedback Recorder — extracts diagnostic paths and records outcomes."""

from __future__ import annotations

from typing import Any

import structlog

from kubortex.shared.models.investigation import InvestigationResult

from .scorer import StrategyRanker

logger = structlog.get_logger(__name__)


def record_feedback(
    ranker: StrategyRanker,
    result: InvestigationResult,
    category: str,
    target_kind: str,
    resolved: bool,
) -> None:
    """Extract the diagnostic path from an investigation result and update scores."""
    path_entries: list[dict[str, Any]] = [
        {"skill": entry.skill, "wasUseful": entry.was_useful} for entry in result.diagnostic_path
    ]

    if not path_entries:
        logger.debug("no_diagnostic_path", category=category, target_kind=target_kind)
        return

    ranker.update_scores(category, target_kind, path_entries, resolved)
    logger.info(
        "feedback_recorded",
        category=category,
        target_kind=target_kind,
        steps=len(path_entries),
        resolved=resolved,
    )
