"""Strategy Ranker — read/update diagnostic scores per (category, targetKind).

Tracks skill success rates, ordering scores, and path pattern frequency.
Uses exponential moving average (EMA) for score decay.
"""

from __future__ import annotations

from typing import Any

import structlog

from kubortex.shared.config import KubortexSettings

from .store import LearningStore

logger = structlog.get_logger(__name__)


class StrategyRanker:
    """Ranks investigation skills based on historical success data."""

    def __init__(self, store: LearningStore) -> None:
        self._store = store
        settings = KubortexSettings()
        self._min_samples = settings.learning_min_samples
        self._alpha = settings.learning_decay_alpha

    def get_hints(self, category: str, target_kind: str) -> dict[str, Any]:
        """Generate diagnostic hints for an investigation.

        Returns a dict with ``preferredSkillOrder`` and ``avoidPaths``.
        Only returns hints after minimum sample threshold is met.
        """
        data = self._store.load(category, target_kind)
        sample_count = data.get("sampleCount", 0)

        if sample_count < self._min_samples:
            return {"preferredSkillOrder": [], "avoidPaths": []}

        skills = data.get("skills", {})
        ranked = sorted(
            skills.items(),
            key=lambda kv: kv[1].get("score", 0),
            reverse=True,
        )

        preferred = [name for name, _ in ranked if skills[name].get("score", 0) > 0.3]
        avoid = [
            name for name, s in skills.items() if s.get("score", 0) < 0.1 and s.get("count", 0) >= 3
        ]

        return {"preferredSkillOrder": preferred, "avoidPaths": avoid}

    def update_scores(
        self,
        category: str,
        target_kind: str,
        diagnostic_path: list[dict[str, Any]],
        resolved: bool,
    ) -> None:
        """Update scores from a completed investigation's diagnostic path."""
        data = self._store.load(category, target_kind)
        skills = data.get("skills", {})
        sample_count = data.get("sampleCount", 0)

        for entry in diagnostic_path:
            skill_name = entry.get("skill", "")
            was_useful = entry.get("wasUseful", False)
            if not skill_name:
                continue

            current = skills.get(skill_name, {"score": 0.5, "count": 0})
            old_score = current["score"]

            # EMA update: score = alpha * new + (1 - alpha) * old
            new_signal = 1.0 if (was_useful and resolved) else 0.0
            new_score = self._alpha * new_signal + (1 - self._alpha) * old_score

            skills[skill_name] = {
                "score": round(new_score, 4),
                "count": current["count"] + 1,
            }

        data["skills"] = skills
        data["sampleCount"] = sample_count + 1
        self._store.save(category, target_kind, data)
        logger.info(
            "diagnostic_scores_updated",
            category=category,
            target_kind=target_kind,
            sample_count=sample_count + 1,
        )
