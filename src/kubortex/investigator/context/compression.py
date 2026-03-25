"""Progressive compression stages for context window management.

Compression is applied in escalating stages when the context budget
is under pressure (SPEC §8.4):
  1. Early evidence summarisation
  2. Skill body eviction
  3. Runbook condensation
  4. Message history truncation
  5. Forced conclusion
"""

from __future__ import annotations

from typing import Any

import structlog

from .budget import ContextBudget

logger = structlog.get_logger(__name__)


def apply_compression(
    budget: ContextBudget,
    evidence: list[dict[str, Any]],
    loaded_skills: set[str],
    loaded_runbook: bool,
    messages: list[Any],
) -> tuple[list[dict[str, Any]], bool]:
    """Apply the next compression stage if budget pressure exists.

    Returns (updated_evidence, should_force_conclude).
    """
    if not budget.needs_compression:
        return evidence, False

    stage = budget.compression_stage + 1
    budget.compression_stage = stage

    if stage == 1:
        evidence = _compress_old_evidence(evidence, budget)
        logger.info("compression_stage_1", action="early_evidence_summarisation")
    elif stage == 2:
        _evict_skill_bodies(loaded_skills, budget)
        logger.info("compression_stage_2", action="skill_body_eviction")
    elif stage == 3:
        if loaded_runbook:
            budget.evict_skill(2000)  # approximate runbook body size
            logger.info("compression_stage_3", action="runbook_condensation")
    elif stage == 4:
        evidence = _truncate_history(evidence, messages, budget)
        logger.info("compression_stage_4", action="message_history_truncation")
    else:
        logger.warning("compression_stage_5", action="forced_conclusion")
        return evidence, True

    return evidence, False


def _compress_old_evidence(
    evidence: list[dict[str, Any]], budget: ContextBudget
) -> list[dict[str, Any]]:
    """Compress older evidence items to one-line references."""
    if len(evidence) <= 2:
        return evidence

    compressed = []
    for i, item in enumerate(evidence):
        if i < len(evidence) - 2:
            summary = item.get("valueSummary", "")
            short = summary[:100] + "..." if len(summary) > 100 else summary
            old_len = len(summary)
            new_len = len(short)
            budget.compress_evidence(old_len, new_len)
            compressed.append({**item, "valueSummary": short})
        else:
            compressed.append(item)
    return compressed


def _evict_skill_bodies(loaded_skills: set[str], budget: ContextBudget) -> None:
    """Remove skill bodies from context tracking."""
    for _skill in list(loaded_skills):
        budget.evict_skill(3000)  # approximate skill body size
    loaded_skills.clear()


def _truncate_history(
    evidence: list[dict[str, Any]],
    messages: list[Any],
    budget: ContextBudget,
) -> list[dict[str, Any]]:
    """Collapse intermediate reasoning into a summary."""
    if len(evidence) > 3:
        evidence = evidence[-3:]
        budget.evidence_chars = sum(len(str(e)) for e in evidence)
    return evidence
