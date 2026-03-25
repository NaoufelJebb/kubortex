"""Pydantic models for runbook manifests."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class RunbookManifest(BaseModel):
    """Metadata parsed from runbook markdown YAML frontmatter."""

    name: str
    description: str
    metadata: dict[str, Any] = Field(default_factory=dict)

    # Derived at registry load time
    body: str = ""  # Full markdown body (loaded lazily)
    file_path: str = ""  # Filesystem path to the runbook file
