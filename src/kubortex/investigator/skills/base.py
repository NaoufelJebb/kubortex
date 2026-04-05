"""Base protocol for all Kubortex investigator skills."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from .models import SkillInput, SkillResult


@runtime_checkable
class BaseSkill(Protocol):
    """Every skill adapter must implement this interface."""

    async def execute(self, inp: SkillInput) -> SkillResult:
        """Run the skill with the given input and return structured results."""
        ...
