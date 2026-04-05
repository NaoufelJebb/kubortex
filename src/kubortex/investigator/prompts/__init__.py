"""Prompt loader for investigator Markdown prompt templates."""

from __future__ import annotations

from pathlib import Path

_DIR = Path(__file__).parent


def load_prompt(name: str) -> str:
    """Load a prompt template by filename from the prompts directory."""
    return (_DIR / name).read_text(encoding="utf-8")
