"""Pydantic models for skill manifests and invocation results."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class SkillManifest(BaseModel):
    """Metadata parsed from SKILL.md YAML frontmatter."""

    name: str
    description: str
    entrypoint: str
    metadata: dict[str, Any] = Field(default_factory=dict)

    # Derived at registry load time
    body: str = ""  # Full markdown body (loaded lazily)
    dir_path: str = ""  # Filesystem path to the skill directory


class SkillInput(BaseModel):
    """Standard input passed to every skill execution."""

    query: str
    namespace: str = ""
    resource_type: str = ""
    parameters: dict[str, Any] = Field(default_factory=dict)


class SkillResult(BaseModel):
    """Standard output returned by every skill execution."""

    success: bool = True
    data: Any = None
    summary: str = ""
    error: str | None = None
    raw_size: int = 0
