"""Token-accurate context budget tracking via tiktoken.

Counts real tokens instead of characters, so utilisation maps directly
to the model's context window.  No section weights — one counter.
"""

from __future__ import annotations

import tiktoken


class ContextBudget:
    """Tracks token usage with tiktoken for accurate context window management."""

    def __init__(self, max_tokens: int = 30_000, model: str = "gpt-4o") -> None:
        try:
            self._enc = tiktoken.encoding_for_model(model)
        except KeyError:
            self._enc = tiktoken.get_encoding("cl100k_base")

        self.max_tokens = max_tokens
        self.used_tokens: int = 0
        self.compression_stage: int = 0

    # ------------------------------------------------------------------
    # Counting helpers
    # ------------------------------------------------------------------

    def count(self, text: str) -> int:
        """Return the token count for *text*."""
        return len(self._enc.encode(text))

    # ------------------------------------------------------------------
    # Mutation
    # ------------------------------------------------------------------

    def add(self, text: str) -> None:
        """Charge *text* against the budget."""
        self.used_tokens += self.count(text)

    def evict(self, text: str) -> None:
        """Reclaim tokens when *text* is removed from context."""
        self.used_tokens = max(0, self.used_tokens - self.count(text))

    def evict_tokens(self, n: int) -> None:
        """Reclaim a known token count directly (use when original text is unavailable)."""
        self.used_tokens = max(0, self.used_tokens - n)
    # ------------------------------------------------------------------
    # Budget queries
    # ------------------------------------------------------------------

    @property
    def remaining(self) -> int:
        return max(0, self.max_tokens - self.used_tokens)

    @property
    def utilisation(self) -> float:
        return self.used_tokens / self.max_tokens if self.max_tokens else 0.0

    @property
    def needs_compression(self) -> bool:
        return self.utilisation > 0.75
