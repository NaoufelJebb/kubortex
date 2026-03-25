"""Heuristic context budget tracking.

Estimates serialized prompt size using character counts and section weights.
No exact token counting — uses heuristic reserves per SPEC §8.3.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ContextBudget:
    """Tracks approximate context usage via character counting."""

    max_chars: int = 120_000

    # Section weights (percentage of max_chars)
    system_overhead_pct: float = 0.15
    skill_runbook_pct: float = 0.15
    evidence_pct: float = 0.50
    reasoning_reserve_pct: float = 0.20

    # Current usage
    system_chars: int = 0
    skill_chars: int = 0
    evidence_chars: int = 0

    # Compression stage tracking
    compression_stage: int = field(default=0)

    @property
    def total_used(self) -> int:
        return self.system_chars + self.skill_chars + self.evidence_chars

    @property
    def remaining(self) -> int:
        usable = int(self.max_chars * (1 - self.reasoning_reserve_pct))
        return max(0, usable - self.total_used)

    @property
    def evidence_remaining(self) -> int:
        evidence_budget = int(self.max_chars * self.evidence_pct)
        return max(0, evidence_budget - self.evidence_chars)

    @property
    def utilisation(self) -> float:
        return self.total_used / self.max_chars if self.max_chars else 0.0

    @property
    def needs_compression(self) -> bool:
        return self.utilisation > 0.75

    def add_system(self, chars: int) -> None:
        self.system_chars += chars

    def add_skill(self, chars: int) -> None:
        self.skill_chars += chars

    def add_evidence(self, chars: int) -> None:
        self.evidence_chars += chars

    def evict_skill(self, chars: int) -> None:
        """Reclaim chars when a skill body is evicted from context."""
        self.skill_chars = max(0, self.skill_chars - chars)

    def compress_evidence(self, old_chars: int, new_chars: int) -> None:
        """Track evidence compression."""
        self.evidence_chars = max(0, self.evidence_chars - old_chars + new_chars)
