"""SkillRegistry — scans skills/ directory, parses SKILL.md frontmatter.

The registry stores compact metadata for Layer 0 prompt injection and
supports lazy loading of the full markdown body (Layer 1).
"""

from __future__ import annotations

from pathlib import Path

import structlog
import yaml

from .models import SkillManifest

logger = structlog.get_logger(__name__)


class SkillRegistry:
    """Filesystem-backed, Pydantic-validated skill manifest registry."""

    def __init__(self) -> None:
        self._skills: dict[str, SkillManifest] = {}

    def load(self, skills_dir: str | Path) -> None:
        """Scan *skills_dir* for ``SKILL.md`` files and parse frontmatter."""
        root = Path(skills_dir)
        if not root.is_dir():
            logger.warning("skills_dir_not_found", path=str(root))
            return

        for skill_md in root.rglob("SKILL.md"):
            try:
                manifest = _parse_skill_md(skill_md)
                self._skills[manifest.name] = manifest
                logger.info("skill_registered", name=manifest.name)
            except Exception:
                logger.exception("skill_parse_error", path=str(skill_md))

    def list_metadata(self) -> list[dict[str, str]]:
        """Return compact metadata for all skills (Layer 0)."""
        return [
            {
                "name": s.name,
                "description": s.description,
                "entrypoint": s.entrypoint,
            }
            for s in self._skills.values()
        ]

    def get(self, name: str) -> SkillManifest | None:
        """Get a skill manifest by name."""
        return self._skills.get(name)

    def get_full_body(self, name: str) -> str:
        """Load and return the full markdown body of a skill (Layer 1)."""
        manifest = self._skills.get(name)
        if not manifest:
            return ""
        if not manifest.body:
            skill_md = Path(manifest.dir_path) / "SKILL.md"
            if skill_md.exists():
                _, body = _split_frontmatter(skill_md.read_text())
                manifest.body = body
        return manifest.body

    @property
    def names(self) -> list[str]:
        return list(self._skills.keys())


def _parse_skill_md(path: Path) -> SkillManifest:
    """Parse a SKILL.md file into a SkillManifest."""
    content = path.read_text()
    frontmatter_str, _body = _split_frontmatter(content)
    data = yaml.safe_load(frontmatter_str)
    manifest = SkillManifest.model_validate(data)
    manifest.dir_path = str(path.parent)
    return manifest


def _split_frontmatter(text: str) -> tuple[str, str]:
    """Split YAML frontmatter (between --- delimiters) from the body."""
    if not text.startswith("---"):
        return "", text
    parts = text.split("---", 2)
    if len(parts) < 3:
        return "", text
    return parts[1].strip(), parts[2].strip()
