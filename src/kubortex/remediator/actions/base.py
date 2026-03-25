"""Base action protocol for all remediation actions.

Every action follows the pipeline: pre_flight → dry_run → execute → verify.
If verification detects regression, rollback is called.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class BaseAction(ABC):
    """Abstract base for remediation actions."""

    @abstractmethod
    async def pre_flight(self, target: dict[str, Any], parameters: dict[str, Any]) -> bool:
        """Validate that the action can proceed.  Returns True if safe."""
        ...

    @abstractmethod
    async def dry_run(self, target: dict[str, Any], parameters: dict[str, Any]) -> dict[str, Any]:
        """Simulate the action without side effects.  Returns expected outcome."""
        ...

    @abstractmethod
    async def execute(self, target: dict[str, Any], parameters: dict[str, Any]) -> dict[str, Any]:
        """Perform the remediation.  Returns execution details."""
        ...

    @abstractmethod
    async def verify(
        self, target: dict[str, Any], parameters: dict[str, Any], execution_result: dict[str, Any]
    ) -> dict[str, Any]:
        """Check that the action had the desired effect.

        Returns ``{"success": bool, "metric": str, "before": ..., "after": ...}``.
        """
        ...

    @abstractmethod
    async def rollback(
        self, target: dict[str, Any], parameters: dict[str, Any], execution_result: dict[str, Any]
    ) -> dict[str, Any]:
        """Reverse the action if verification fails."""
        ...
