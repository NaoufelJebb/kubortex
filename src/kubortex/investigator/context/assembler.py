"""Context Assembler — builds layered prompts for the investigator.

Implements the Progressive Disclosure Design Principle (SPEC §8.2):
  Layer 0: Always present (system, incident, skill/runbook metadata)
  Layer 1: Loaded on first use (full SKILL.md body)
  Layer 2: Loaded on demand (runbook strategy body)
  Layer 3: Ephemeral (summarised skill results)
"""

from __future__ import annotations

from typing import Any

from kubortex.investigator.runbooks.models import RunbookManifest
from kubortex.investigator.runbooks.registry import RunbookRegistry
from kubortex.investigator.skills.registry import SkillRegistry

from .budget import ContextBudget


class ContextAssembler:
    """Dynamically constructs investigation prompts with budget tracking."""

    def __init__(
        self,
        skill_registry: SkillRegistry,
        runbook_registry: RunbookRegistry,
        max_tokens: int = 30_000,
        model: str = "gpt-4o",
    ) -> None:
        self._skills = skill_registry
        self._runbooks = runbook_registry
        self.budget = ContextBudget(max_tokens=max_tokens, model=model)
        self.loaded_skills: set[str] = set()
        self.loaded_runbook: bool = False
        self._matched_runbook: RunbookManifest | None = None

    def build_initial_prompt(
        self,
        incident_context: dict[str, Any],
        diagnostic_hints: dict[str, Any] | None = None,
    ) -> str:
        """Build the Layer 0 context prompt (incident + manifests + hints).

        The system role instructions are loaded separately from SYSTEM_PROMPT.md
        and prepended by the initialise node.
        """
        sections = [
            _incident_section(incident_context),
            _skill_manifest_section(self._skills),
            _runbook_manifest_section(self._runbooks),
        ]

        if diagnostic_hints:
            sections.append(_diagnostic_hints_section(diagnostic_hints))

        prompt = "\n\n".join(sections)
        self.budget.add(prompt)
        return prompt

    def inject_skill_body(self, skill_name: str) -> str | None:
        """Lazy-load a skill's full body (Layer 1). Returns body or None."""
        if skill_name in self.loaded_skills:
            return None
        body = self._skills.get_full_body(skill_name)
        if body:
            self.loaded_skills.add(skill_name)
            self.budget.add(body)
            return body
        return None

    def inject_runbook_body(self, runbook_name: str) -> str | None:
        """Lazy-load a runbook's full body (Layer 2). Returns body or None."""
        if self.loaded_runbook:
            return None
        body = self._runbooks.get_full_body(runbook_name)
        if body:
            self.loaded_runbook = True
            self.budget.add(body)
            return body
        return None

    def add_evidence(self, summary: str) -> None:
        """Track evidence added to context (Layer 3)."""
        self.budget.add(summary)

    @property
    def matched_runbook(self) -> RunbookManifest | None:
        return self._matched_runbook

    @matched_runbook.setter
    def matched_runbook(self, value: RunbookManifest | None) -> None:
        self._matched_runbook = value


# ---------------------------------------------------------------------------
# Prompt section builders
# ---------------------------------------------------------------------------


def _incident_section(ctx: dict[str, Any]) -> str:
    lines = ["## Incident Context"]
    for key in ["summary", "severity", "category"]:
        if key in ctx:
            lines.append(f"- **{key}**: {ctx[key]}")

    target = ctx.get("targetRef")
    if target:
        kind = target.get("kind")
        ns = target.get("namespace")
        n = target.get("name")
        lines.append(f"- **target**: {kind}/{ns}/{n}")

    signals = ctx.get("signals", [])
    if signals:
        lines.append(f"\n### Signals ({len(signals)})")
        for s in signals[:5]:
            lines.append(f"- [{s.get('severity')}] {s.get('alertname')}: {s.get('summary')}")
    return "\n".join(lines)


def _skill_manifest_section(registry: SkillRegistry) -> str:
    lines = ["## Available Skills"]
    for meta in registry.list_metadata():
        lines.append(f"- **{meta['name']}**: {meta['description']}")
    return "\n".join(lines)


def _runbook_manifest_section(registry: RunbookRegistry) -> str:
    lines = ["## Available Runbooks"]
    for meta in registry.list_metadata():
        lines.append(f"- **{meta['name']}**: {meta['description']}")
    return "\n".join(lines)


def _diagnostic_hints_section(hints: dict[str, Any]) -> str:
    lines = ["## Diagnostic Hints (from past investigations)"]
    order = hints.get("preferredSkillOrder", [])
    if order:
        lines.append(f"- Recommended skill order: {', '.join(order)}")
    avoid = hints.get("avoidPaths", [])
    if avoid:
        lines.append(f"- Patterns to avoid: {', '.join(avoid)}")
    return "\n".join(lines)
