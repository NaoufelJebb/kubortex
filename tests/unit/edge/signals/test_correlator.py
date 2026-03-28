"""Unit tests for kubortex.edge.signals.correlator (pure helpers)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from kubortex.edge.signals.correlator import (
    _correlation_key,
    _find_active_incident,
    _highest_severity,
    _incident_name,
    correlate_and_upsert,
)
from kubortex.shared.types import Category, Severity

from ..conftest import make_incident_obj, make_signal, make_target_ref

# ---------------------------------------------------------------------------
# _correlation_key
# ---------------------------------------------------------------------------


class TestCorrelationKey:
    def test_with_target(self, target_ref) -> None:
        key = _correlation_key(Category.LATENCY, target_ref)
        assert key == f"{Category.LATENCY}:default/my-app"

    def test_without_target(self) -> None:
        key = _correlation_key(Category.LATENCY, None)
        assert key == f"{Category.LATENCY}:/"

    def test_key_is_deterministic(self, target_ref) -> None:
        assert _correlation_key(Category.ERROR_RATE, target_ref) == _correlation_key(
            Category.ERROR_RATE, target_ref
        )


# ---------------------------------------------------------------------------
# _incident_name
# ---------------------------------------------------------------------------


class TestIncidentName:
    def test_returns_inc_prefix(self) -> None:
        name = _incident_name("some:key")
        assert name.startswith("inc-")

    def test_digest_length_is_8(self) -> None:
        name = _incident_name("some:key")
        # format: inc-YYYYMMDDHHMMSSffffff-XXXXXXXX
        parts = name.split("-")
        assert len(parts) == 3
        assert len(parts[2]) == 8

    def test_same_key_same_digest(self) -> None:
        n1 = _incident_name("a:b/c")
        n2 = _incident_name("a:b/c")
        # digest portion is deterministic regardless of call time.
        assert n1.split("-")[2] == n2.split("-")[2]

    def test_different_keys_produce_different_digests(self) -> None:
        d1 = _incident_name("latency:ns/app1").split("-")[2]
        d2 = _incident_name("latency:ns/app2").split("-")[2]
        assert d1 != d2

    def test_same_key_produces_unique_name(self) -> None:
        n1 = _incident_name("a:b/c")
        n2 = _incident_name("a:b/c")
        assert n1 != n2


# ---------------------------------------------------------------------------
# _highest_severity
# ---------------------------------------------------------------------------


class TestHighestSeverity:
    def test_single_signal(self) -> None:
        assert _highest_severity([make_signal(severity=Severity.INFO)]) == Severity.INFO

    def test_mixed_signals_returns_highest(self) -> None:
        signals = [
            make_signal(severity=Severity.INFO),
            make_signal(severity=Severity.CRITICAL),
            make_signal(severity=Severity.WARNING),
        ]
        assert _highest_severity(signals) == Severity.CRITICAL

    def test_all_same_severity(self) -> None:
        signals = [make_signal(severity=Severity.HIGH)] * 3
        assert _highest_severity(signals) == Severity.HIGH

    def test_severity_order_respects_enum_definition(self) -> None:
        # INFO < WARNING < HIGH < CRITICAL in Severity enum order
        signals = [make_signal(severity=Severity.WARNING), make_signal(severity=Severity.HIGH)]
        assert _highest_severity(signals) == Severity.HIGH


# ---------------------------------------------------------------------------
# _find_active_incident
# ---------------------------------------------------------------------------


class TestFindActiveIncident:
    @pytest.mark.asyncio
    async def test_returns_none_when_no_incidents(self, mock_k8s) -> None:
        mock_k8s["list_resources"].return_value = []
        result = await _find_active_incident(Category.LATENCY, None, "default", 300)
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_matching_incident(self, mock_k8s) -> None:
        inc = make_incident_obj(category=Category.LATENCY)
        mock_k8s["list_resources"].return_value = [inc]
        result = await _find_active_incident(Category.LATENCY, None, "default", 300)
        assert result is not None
        assert result["metadata"]["name"] == inc["metadata"]["name"]

    @pytest.mark.asyncio
    async def test_skips_terminal_phases(self, mock_k8s) -> None:
        for phase in ("Resolved", "Escalated", "Suppressed"):
            inc = make_incident_obj(category=Category.LATENCY, phase=phase)
            mock_k8s["list_resources"].return_value = [inc]
            assert await _find_active_incident(Category.LATENCY, None, "default", 300) is None

    @pytest.mark.asyncio
    async def test_skips_incidents_outside_correlation_window(self, mock_k8s) -> None:
        old_ts = (
            datetime.now(UTC) - timedelta(seconds=300 + 60)
        ).isoformat()
        inc = make_incident_obj(
            category=Category.LATENCY,
            creation_timestamp=old_ts,
        )
        mock_k8s["list_resources"].return_value = [inc]
        assert await _find_active_incident(Category.LATENCY, None, "default", 300) is None

    @pytest.mark.asyncio
    async def test_skips_mismatched_category(self, mock_k8s) -> None:
        inc = make_incident_obj(category=Category.ERROR_RATE)
        mock_k8s["list_resources"].return_value = [inc]
        assert await _find_active_incident(Category.LATENCY, None, "default", 300) is None

    @pytest.mark.asyncio
    async def test_skips_incident_with_mismatched_target_ref(self, mock_k8s) -> None:
        inc = make_incident_obj(
            category=Category.LATENCY,
            target_ref=make_target_ref(name="api"),
        )
        mock_k8s["list_resources"].return_value = [inc]

        result = await _find_active_incident(
            Category.LATENCY,
            make_target_ref(name="worker"),
            "default",
            300,
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_matches_incident_with_same_target_ref(self, mock_k8s) -> None:
        target = make_target_ref(name="api")
        inc = make_incident_obj(category=Category.LATENCY, target_ref=target)
        mock_k8s["list_resources"].return_value = [inc]

        result = await _find_active_incident(Category.LATENCY, target, "default", 300)

        assert result is not None
        assert result["metadata"]["name"] == inc["metadata"]["name"]

    @pytest.mark.asyncio
    async def test_targetless_signal_does_not_match_targeted_incident(self, mock_k8s) -> None:
        inc = make_incident_obj(
            category=Category.LATENCY,
            target_ref=make_target_ref(name="api"),
        )
        mock_k8s["list_resources"].return_value = [inc]

        result = await _find_active_incident(Category.LATENCY, None, "default", 300)

        assert result is None


# ---------------------------------------------------------------------------
# correlate_and_upsert
# ---------------------------------------------------------------------------


class TestCorrelateAndUpsert:
    @pytest.mark.asyncio
    async def test_creates_new_incident_when_none_active(self, mock_k8s) -> None:
        signals = [make_signal(severity=Severity.HIGH, summary="CPU spike")]
        name = await correlate_and_upsert(
            signals, Category.RESOURCE_SATURATION, None, "kubortex-system"
        )
        assert name.startswith("inc-")
        mock_k8s["create_resource"].assert_awaited_once()

    @pytest.mark.asyncio
    async def test_updates_existing_incident(self, mock_k8s) -> None:
        inc = make_incident_obj(name="inc-existing", category=Category.RESOURCE_SATURATION)
        mock_k8s["list_resources"].return_value = [inc]
        existing = make_signal(alertname="OldAlert")
        mock_k8s["get_resource"].return_value = {
            "spec": {"signals": [existing.model_dump(by_alias=True, mode="json")]}
        }

        new_signal = make_signal(alertname="NewAlert")
        name = await correlate_and_upsert(
            [new_signal], Category.RESOURCE_SATURATION, None, "kubortex-system"
        )
        assert name == "inc-existing"
        mock_k8s["create_resource"].assert_not_awaited()

        # signals must be written back to spec with both old and new entries
        mock_k8s["patch_spec"].assert_awaited_once()
        spec_call = mock_k8s["patch_spec"].await_args
        merged_signals = spec_call[0][2]["signals"]  # (plural, name, spec_patch)
        assert len(merged_signals) == 2
        alertnames = {s["alertname"] for s in merged_signals}
        assert alertnames == {"OldAlert", "NewAlert"}

        mock_k8s["patch_status"].assert_awaited_once()

    @pytest.mark.asyncio
    async def test_incident_body_has_correct_fields(self, mock_k8s) -> None:
        signals = [make_signal(severity=Severity.CRITICAL, summary="Disk full")]
        await correlate_and_upsert(
            signals, Category.RESOURCE_SATURATION, make_target_ref(), "kubortex-system"
        )
        call_args = mock_k8s["create_resource"].await_args
        body = call_args[0][1]  # positional arg: (plural, body)
        assert body["kind"] == "Incident"
        assert body["spec"]["severity"] == Severity.CRITICAL
        assert body["spec"]["category"] == Category.RESOURCE_SATURATION
        assert body["spec"]["summary"] == "Disk full"
        assert body["spec"]["targetRef"] is not None

    @pytest.mark.asyncio
    async def test_incident_without_target_has_null_target_ref(self, mock_k8s) -> None:
        signals = [make_signal()]
        await correlate_and_upsert(signals, Category.CUSTOM, None, "kubortex-system")
        body = mock_k8s["create_resource"].await_args[0][1]
        assert body["spec"]["targetRef"] is None

    @pytest.mark.asyncio
    async def test_empty_signals_list_still_creates_incident(self, mock_k8s) -> None:
        name = await correlate_and_upsert([], Category.CUSTOM, None, "kubortex-system")
        assert name.startswith("inc-")
        body = mock_k8s["create_resource"].await_args[0][1]
        assert body["spec"]["summary"] == "Unknown incident"
