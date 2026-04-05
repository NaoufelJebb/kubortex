"""Unit tests for SkillRegistry."""

from __future__ import annotations

from pathlib import Path

import pytest

from kubortex.investigator.skills.registry import SkillRegistry


def _write_skill_md(skill_dir: Path, name: str, entrypoint: str) -> None:
    skill_dir.mkdir(parents=True, exist_ok=True)
    skill_md = skill_dir / "SKILL.md"
    skill_md.write_text(
        f"---\nname: {name}\ndescription: Test skill {name}\nentrypoint: {entrypoint}\n---\n\n"
        f"# {name}\n\nFull body text for {name}."
    )


def test_load_nonexistent_dir_does_not_raise(tmp_path: Path) -> None:
    registry = SkillRegistry()
    registry.load(tmp_path / "does_not_exist")
    assert registry.names == []


def test_load_finds_and_registers_skills(tmp_path: Path) -> None:
    _write_skill_md(tmp_path / "alpha", "alpha", "skills.alpha.src.alpha.create")
    _write_skill_md(tmp_path / "beta", "beta", "skills.beta.src.beta.create")

    registry = SkillRegistry()
    registry.load(tmp_path)

    assert set(registry.names) == {"alpha", "beta"}


def test_get_returns_manifest_by_name(tmp_path: Path) -> None:
    _write_skill_md(tmp_path / "myskill", "myskill", "skills.myskill.src.myskill.create")

    registry = SkillRegistry()
    registry.load(tmp_path)

    manifest = registry.get("myskill")
    assert manifest is not None
    assert manifest.name == "myskill"
    assert manifest.entrypoint == "skills.myskill.src.myskill.create"


def test_get_returns_none_for_unknown() -> None:
    registry = SkillRegistry()
    assert registry.get("nonexistent") is None


def test_names_property_lists_all(tmp_path: Path) -> None:
    for name in ("x", "y", "z"):
        _write_skill_md(tmp_path / name, name, f"skills.{name}.src.{name}.create")

    registry = SkillRegistry()
    registry.load(tmp_path)

    assert sorted(registry.names) == ["x", "y", "z"]


def test_get_full_body_loads_markdown(tmp_path: Path) -> None:
    _write_skill_md(tmp_path / "docs", "docs", "skills.docs.src.docs.create")

    registry = SkillRegistry()
    registry.load(tmp_path)

    body = registry.get_full_body("docs")
    assert "Full body text for docs" in body


def test_get_full_body_returns_empty_for_unknown() -> None:
    registry = SkillRegistry()
    assert registry.get_full_body("nope") == ""
