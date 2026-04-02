"""Capability Gateway — sits between the LLM and skill implementations.

Validates inputs, enforces scope, applies rate limits, executes skills,
records telemetry, and applies output processing.
"""

from __future__ import annotations

import importlib
import time

import structlog

from kubortex.shared.models import SkillInput, SkillInvocationRecord, SkillManifest, SkillResult

from .base import BaseSkill
from .registry import SkillRegistry

logger = structlog.get_logger(__name__)

# Default truncation limit for skill output
DEFAULT_MAX_OUTPUT_CHARS = 50_000


class CapabilityGateway:
    """Validates, executes, and audits skill invocations."""

    def __init__(self, registry: SkillRegistry) -> None:
        self._registry = registry
        self._adapters: dict[str, BaseSkill] = {}
        self._invocation_counts: dict[str, int] = {}  # per investigation

    def reset_counts(self) -> None:
        """Reset per-investigation invocation counts."""
        self._invocation_counts.clear()

    async def invoke(
        self,
        skill_name: str,
        inp: SkillInput,
        *,
        allowed_namespaces: list[str] | None = None,
    ) -> tuple[SkillResult, SkillInvocationRecord]:
        """Execute a skill through the gateway with full audit trail.

        Returns (result, telemetry_record).
        """
        manifest = self._registry.get(skill_name)
        if not manifest:
            return _error_result(f"unknown skill: {skill_name}"), _record(
                skill_name, 0, 0, f"unknown skill: {skill_name}"
            )

        # Scope enforcement
        if allowed_namespaces and inp.namespace and inp.namespace not in allowed_namespaces:
            msg = f"namespace '{inp.namespace}' not in allowed scope"
            return _error_result(msg), _record(skill_name, 0, 0, msg)

        # Rate limit check
        rate_limit = manifest.metadata.get("rateLimit", 20)
        count = self._invocation_counts.get(skill_name, 0)
        if count >= rate_limit:
            msg = f"rate limit exceeded for skill '{skill_name}'"
            return _error_result(msg), _record(skill_name, 0, 0, msg)

        # Resolve adapter
        adapter = self._resolve_adapter(manifest)
        if not adapter:
            msg = f"failed to load adapter for '{skill_name}'"
            return _error_result(msg), _record(skill_name, 0, 0, msg)

        # Execute
        start = time.monotonic()
        try:
            result = await adapter.execute(inp)
        except Exception as exc:
            latency = (time.monotonic() - start) * 1000
            msg = f"skill execution error: {exc}"
            logger.exception("skill_error", skill=skill_name)
            return _error_result(msg), _record(skill_name, latency, 0, msg)

        latency = (time.monotonic() - start) * 1000

        # Output truncation
        max_chars = manifest.metadata.get("maxOutputChars", DEFAULT_MAX_OUTPUT_CHARS)
        if result.summary and len(result.summary) > max_chars:
            result.summary = result.summary[:max_chars] + "\n... [truncated]"

        self._invocation_counts[skill_name] = count + 1
        record = _record(skill_name, latency, result.raw_size)
        logger.info(
            "skill_invoked",
            skill=skill_name,
            latency_ms=round(latency, 1),
            output_size=result.raw_size,
        )
        return result, record

    def _resolve_adapter(self, manifest: SkillManifest) -> BaseSkill | None:
        """Dynamically load the skill adapter from its entrypoint."""
        if manifest.name in self._adapters:
            return self._adapters[manifest.name]

        try:
            module_path, func_name = manifest.entrypoint.rsplit(".", 1)
            module = importlib.import_module(module_path)
            factory = getattr(module, func_name)
            adapter = factory()
            self._adapters[manifest.name] = adapter
            return adapter
        except Exception:
            logger.exception("adapter_load_error", entrypoint=manifest.entrypoint)
            return None


def _error_result(msg: str) -> SkillResult:
    return SkillResult(success=False, error=msg)


def _record(
    skill: str, latency_ms: float, output_size: int, error: str | None = None
) -> SkillInvocationRecord:
    return SkillInvocationRecord(
        skill=skill,
        latencyMs=latency_ms,
        outputSize=output_size,
        error=error,
    )
