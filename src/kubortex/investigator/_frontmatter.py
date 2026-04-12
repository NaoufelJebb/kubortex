"""YAML frontmatter parsing shared by skill and runbook registries."""

from __future__ import annotations


def split_frontmatter(text: str) -> tuple[str, str]:
    """Split YAML frontmatter (between ``---`` delimiters) from the body.

    Args:
        text: Raw markdown text potentially starting with ``---``.

    Returns:
        Tuple of (frontmatter_string, body_string).  When no frontmatter
        is detected, frontmatter_string is empty and body_string is the
        full input.
    """
    if not text.startswith("---"):
        return "", text
    parts = text.split("---", 2)
    if len(parts) < 3:
        return "", text
    return parts[1].strip(), parts[2].strip()
