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

# Token approximations used when the original text is no longer accessible.
# Derived from typical SKILL.md and runbook body sizes at ~4 chars/token.
_APPROX_SKILL_TOKENS = 750
_APPROX_RUNBOOK_TOKENS = 500


def apply_compression(
    budget: ContextBudget,
    evidence: list[dict[str, Any]],
    loaded_skills: set[str],
    loaded_runbook: bool,
    messages: list[Any],
    injected_message_ids: list[str] | None = None,
) -> tuple[list[dict[str, Any]], bool, list[str]]:
    """Apply the next compression stage if budget pressure exists.

    Returns (updated_evidence, should_force_conclude, message_ids_to_evict).
    """
    if not budget.needs_compression:
        return evidence, False, []

    stage = budget.compression_stage + 1
    budget.compression_stage = stage

    if stage == 1:
        evidence = _compress_old_evidence(evidence, budget)
        logger.info("compression_stage_1", action="early_evidence_summarisation")
    elif stage == 2:
        _evict_skill_bodies(loaded_skills, budget)
        logger.info("compression_stage_2", action="skill_body_eviction")
        return evidence, False, list(injected_message_ids or [])
    elif stage == 3:
        if loaded_runbook:
            budget.evict_tokens(_APPROX_RUNBOOK_TOKENS)
            logger.info("compression_stage_3", action="runbook_condensation")
    elif stage == 4:
        evidence = _truncate_history(evidence, budget)
        logger.info("compression_stage_4", action="message_history_truncation")
    else:
        logger.warning("compression_stage_5", action="forced_conclusion")
        return evidence, True, []

    return evidence, False, []


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
            budget.evict(summary)
            budget.add(short)
            compressed.append({**item, "valueSummary": short})
        else:
            compressed.append(item)
    return compressed


def _evict_skill_bodies(loaded_skills: set[str], budget: ContextBudget) -> None:
    """Remove skill bodies from context tracking.

    Uses a token approximation since the original body text is not retained
    after injection into the message list.
    """
    for _ in loaded_skills:
        budget.evict_tokens(_APPROX_SKILL_TOKENS)
    loaded_skills.clear()


def _truncate_history(
    evidence: list[dict[str, Any]],
    budget: ContextBudget,
) -> list[dict[str, Any]]:
    """Keep only the most recent evidence items, reclaiming budget for the rest."""
    if len(evidence) <= 3:
        return evidence

    evicted = evidence[:-3]
    for item in evicted:
        budget.evict(item.get("valueSummary", ""))
    return evidence[-3:]
