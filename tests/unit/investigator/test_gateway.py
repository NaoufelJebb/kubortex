"""Unit tests for CapabilityGateway."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kubortex.investigator.skills.gateway import CapabilityGateway
from kubortex.investigator.skills.models import SkillInput, SkillManifest, SkillResult
from kubortex.investigator.skills.registry import SkillRegistry


def _make_registry(skill_name: str = "kube-query") -> SkillRegistry:
    registry = MagicMock(spec=SkillRegistry)
    manifest = SkillManifest(
        name=skill_name,
        description="Test skill",
        entrypoint="skills.kube_query.src.kube_query.create",
        metadata={"rateLimit": 5, "maxOutputChars": 100},
    )
    registry.get.return_value = manifest
    return registry


def _make_input(query: str = "list pods") -> SkillInput:
    return SkillInput(query=query, namespace="default")


@pytest.mark.asyncio
async def test_invoke_unknown_skill_returns_error_result() -> None:
    registry = MagicMock(spec=SkillRegistry)
    registry.get.return_value = None

    gateway = CapabilityGateway(registry=registry)
    result, record = await gateway.invoke("nonexistent", _make_input())

    assert not result.success
    assert "unknown skill" in result.error
    assert record.skill == "nonexistent"
    assert record.error is not None


@pytest.mark.asyncio
async def test_invoke_rate_limit_exceeded_returns_error() -> None:
    registry = _make_registry()
    gateway = CapabilityGateway(registry=registry)

    # Exhaust the rate limit (manifest sets rateLimit=5)
    with patch.object(gateway, "_resolve_adapter") as mock_resolve:
        adapter = MagicMock()
        adapter.execute = AsyncMock(return_value=SkillResult(success=True, summary="ok"))
        mock_resolve.return_value = adapter

        for _ in range(5):
            await gateway.invoke("kube-query", _make_input())

        result, record = await gateway.invoke("kube-query", _make_input())

    assert not result.success
    assert "rate limit" in result.error
    assert record.error is not None


@pytest.mark.asyncio
async def test_invoke_adapter_load_failure_returns_error() -> None:
    registry = _make_registry()
    gateway = CapabilityGateway(registry=registry)

    with patch.object(gateway, "_resolve_adapter", return_value=None):
        result, record = await gateway.invoke("kube-query", _make_input())

    assert not result.success
    assert "failed to load adapter" in result.error


@pytest.mark.asyncio
async def test_invoke_successful_returns_result_and_record() -> None:
    registry = _make_registry()
    gateway = CapabilityGateway(registry=registry)

    adapter = MagicMock()
    adapter.execute = AsyncMock(
        return_value=SkillResult(success=True, summary="pod list", data={"pods": []}, raw_size=20)
    )
    with patch.object(gateway, "_resolve_adapter", return_value=adapter):
        result, record = await gateway.invoke("kube-query", _make_input())

    assert result.success
    assert result.summary == "pod list"
    assert record.skill == "kube-query"
    assert record.error is None
    assert record.latency_ms >= 0


@pytest.mark.asyncio
async def test_invoke_increments_invocation_count() -> None:
    registry = _make_registry()
    gateway = CapabilityGateway(registry=registry)

    adapter = MagicMock()
    adapter.execute = AsyncMock(return_value=SkillResult(success=True, summary="ok"))

    with patch.object(gateway, "_resolve_adapter", return_value=adapter):
        await gateway.invoke("kube-query", _make_input())
        await gateway.invoke("kube-query", _make_input())

    assert gateway._invocation_counts.get("kube-query") == 2


@pytest.mark.asyncio
async def test_invoke_truncates_output_exceeding_max_chars() -> None:
    registry = _make_registry()  # maxOutputChars=100
    gateway = CapabilityGateway(registry=registry)

    long_summary = "x" * 200
    adapter = MagicMock()
    adapter.execute = AsyncMock(
        return_value=SkillResult(success=True, summary=long_summary)
    )

    with patch.object(gateway, "_resolve_adapter", return_value=adapter):
        result, _ = await gateway.invoke("kube-query", _make_input())

    assert len(result.summary) <= 100 + len("\n... [truncated]")
    assert result.summary.endswith("\n... [truncated]")


def test_reset_counts_clears_state() -> None:
    registry = _make_registry()
    gateway = CapabilityGateway(registry=registry)
    gateway._invocation_counts["kube-query"] = 3

    gateway.reset_counts()

    assert gateway._invocation_counts == {}
