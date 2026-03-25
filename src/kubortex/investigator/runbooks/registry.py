"""RunbookRegistry — scans runbooks/ directory, parses frontmatter.

Stores compact metadata for prompt injection and supports lazy loading
of the full markdown body.
"""

from __future__ import annotations

from pathlib import Path

import structlog
import yaml

from .models import RunbookManifest

logger = structlog.get_logger(__name__)


class RunbookRegistry:
    """Filesystem-backed, Pydantic-validated runbook manifest registry."""

    def __init__(self) -> None:
        self._runbooks: dict[str, RunbookManifest] = {}

    def load(self, runbooks_dir: str | Path) -> None:
        """Scan *runbooks_dir* for markdown files and parse frontmatter."""
        root = Path(runbooks_dir)
        if not root.is_dir():
            logger.warning("runbooks_dir_not_found", path=str(root))
            return

        for md_path in root.rglob("*.md"):
            try:
                manifest = _parse_runbook_md(md_path)
                self._runbooks[manifest.name] = manifest
                logger.info("runbook_registered", name=manifest.name)
            except Exception:
                logger.exception("runbook_parse_error", path=str(md_path))

    def list_metadata(self) -> list[dict[str, str]]:
        """Return compact metadata for all runbooks (Layer 0)."""
        return [{"name": r.name, "description": r.description} for r in self._runbooks.values()]

    def get(self, name: str) -> RunbookManifest | None:
        """Get a runbook manifest by name."""
        return self._runbooks.get(name)

    def get_full_body(self, name: str) -> str:
        """Load and return the full markdown body of a runbook (Layer 2)."""
        manifest = self._runbooks.get(name)
        if not manifest:
            return ""
        if not manifest.body:
            path = Path(manifest.file_path)
            if path.exists():
                _, body = _split_frontmatter(path.read_text())
                manifest.body = body
        return manifest.body

    @property
    def names(self) -> list[str]:
        return list(self._runbooks.keys())

    @property
    def all(self) -> list[RunbookManifest]:
        return list(self._runbooks.values())


def _parse_runbook_md(path: Path) -> RunbookManifest:
    """Parse a runbook markdown file into a RunbookManifest."""
    content = path.read_text()
    frontmatter_str, _body = _split_frontmatter(content)
    data = yaml.safe_load(frontmatter_str)
    manifest = RunbookManifest.model_validate(data)
    manifest.file_path = str(path)
    return manifest


def _split_frontmatter(text: str) -> tuple[str, str]:
    """Split YAML frontmatter (between --- delimiters) from the body."""
    if not text.startswith("---"):
        return "", text
    parts = text.split("---", 2)
    if len(parts) < 3:
        return "", text
    return parts[1].strip(), parts[2].strip()
