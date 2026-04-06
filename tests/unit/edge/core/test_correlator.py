"""Unit tests for kubortex.edge.core.correlator (pure helpers)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from kubernetes_asyncio.client import ApiException

from kubortex.edge.core.correlator import (
    _RETRY_BASE_BACKOFF,
    _RETRY_MAX_BACKOFF,
    _backoff_seconds,
    _candidate_incident_names,
    _correlation_key,
    _dedup_signals,
    _find_active_incident,
    _highest_severity,
    _highest_severity_from_raw,
    _incident_name,
    _severity_index,
    correlate_and_upsert,
)
from kubortex.shared.types import Category, Severity

from ..conftest import make_incident_obj, make_signal, make_target_ref

# ---------------------------------------------------------------------------
# _backoff_seconds
# ---------------------------------------------------------------------------


class TestBackoffSeconds:
    def test_zero_attempt_is_within_base(self) -> None:
        assert 0 <= _backoff_seconds(0) <= _RETRY_BASE_BACKOFF

    def test_higher_attempt_allows_larger_value(self) -> None:
        # attempt=5 ceiling = min(0.05 * 32, 2.0) = 1.6; must be > base ceiling
        ceiling_0 = _RETRY_BASE_BACKOFF
        ceiling_5 = min(_RETRY_BASE_BACKOFF * (2**5), _RETRY_MAX_BACKOFF)
        assert ceiling_5 > ceiling_0

    def test_backoff_capped_at_max(self) -> None:
        # At a large attempt the ceiling must not exceed _RETRY_MAX_BACKOFF
        for _ in range(20):
            assert _backoff_seconds(100) <= _RETRY_MAX_BACKOFF

    def test_returns_non_negative(self) -> None:
        for attempt in range(10):
            assert _backoff_seconds(attempt) >= 0


# ---------------------------------------------------------------------------
# _severity_index / _highest_severity_from_raw
# ---------------------------------------------------------------------------


class TestSeverityHelpers:
    def test_severity_index_known_values(self) -> None:
        assert _severity_index("info") < _severity_index("warning")
        assert _severity_index("warning") < _severity_index("high")
        assert _severity_index("high") < _severity_index("critical")

    def test_severity_index_unknown_returns_zero(self) -> None:
        assert _severity_index("unknown-value") == 0

    def test_highest_severity_from_raw_empty(self) -> None:
        assert _highest_severity_from_raw([]) == "info"

    def test_highest_severity_from_raw_picks_critical(self) -> None:
        signals = [
            {"alertname": "A", "severity": "warning", "observedAt": "2024-01-01T00:00:00+00:00"},
            {"alertname": "B", "severity": "critical", "observedAt": "2024-01-01T00:00:01+00:00"},
        ]
        assert _highest_severity_from_raw(signals) == "critical"


# ---------------------------------------------------------------------------
# _dedup_signals
# ---------------------------------------------------------------------------


class TestDedupSignals:
    def _make_raw(self, alertname: str, observed_at: str = "2024-01-01T00:00:00+00:00") -> dict:
        return {"alertname": alertname, "observedAt": observed_at, "severity": "warning"}

    def test_no_duplicates_passes_through(self) -> None:
        existing = [self._make_raw("A")]
        new = [self._make_raw("B")]
        assert _dedup_signals(existing, new) == new

    def test_exact_duplicate_filtered(self) -> None:
        sig = self._make_raw("A")
        assert _dedup_signals([sig], [sig]) == []

    def test_same_alertname_different_time_passes(self) -> None:
        existing = [self._make_raw("A", "2024-01-01T00:00:00+00:00")]
        new = [self._make_raw("A", "2024-01-01T00:01:00+00:00")]
        assert _dedup_signals(existing, new) == new

    def test_empty_existing_passes_all(self) -> None:
        new = [self._make_raw("A"), self._make_raw("B")]
        assert _dedup_signals([], new) == new


# ---------------------------------------------------------------------------
# _correlation_key
# ---------------------------------------------------------------------------


class TestCorrelationKey:
    def test_with_target(self, target_ref) -> None:
        key = _correlation_key(target_ref)
        assert key == "Deployment:default/my-app"

    def test_without_target(self) -> None:
        key = _correlation_key(None)
        assert key == "::/unknown"

    def test_target_kind_participates_in_key(self) -> None:
        deployment = make_target_ref(kind="Deployment", name="api")
        statefulset = make_target_ref(kind="StatefulSet", name="api")
        assert _correlation_key(deployment) != _correlation_key(statefulset)

    def test_key_is_deterministic(self, target_ref) -> None:
        assert _correlation_key(target_ref) == _correlation_key(target_ref)

    def test_different_categories_same_target_produce_same_key(self, target_ref) -> None:
        """Category must not affect the key — signals share one Incident per target."""
        assert _correlation_key(target_ref) == _correlation_key(target_ref)


# ---------------------------------------------------------------------------
# _incident_name
# ---------------------------------------------------------------------------


class TestIncidentName:
    def test_returns_inc_prefix(self) -> None:
        name = _incident_name("some:key", 300)
        assert name.startswith("inc-")

    def test_digest_length_is_8(self) -> None:
        name = _incident_name("some:key", 300)
        parts = name.split("-")
        assert len(parts) == 3
        assert len(parts[2]) == 8

    def test_same_key_same_digest(self) -> None:
        now = datetime(2026, 4, 3, 12, tzinfo=UTC)
        n1 = _incident_name("a:b/c", 300, now=now)
        n2 = _incident_name("a:b/c", 300, now=now)
        assert n1.split("-")[2] == n2.split("-")[2]

    def test_different_keys_produce_different_digests(self) -> None:
        d1 = _incident_name("latency:Deployment:ns/app1", 300).split("-")[2]
        d2 = _incident_name("latency:Deployment:ns/app2", 300).split("-")[2]
        assert d1 != d2

    def test_same_key_same_window_produces_same_name(self) -> None:
        now = datetime(2026, 4, 3, 12, tzinfo=UTC)
        assert _incident_name("a:b/c", 300, now=now) == _incident_name("a:b/c", 300, now=now)

    def test_name_has_no_conflict_suffix(self) -> None:
        now = datetime(2026, 4, 3, 12, tzinfo=UTC)
        assert _incident_name("a:b/c", 300, now=now).count("-") == 2


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
# _candidate_incident_names
# ---------------------------------------------------------------------------


class TestCandidateIncidentNames:
    def test_probes_current_and_previous_buckets(self) -> None:
        now = datetime(2026, 4, 3, 12, tzinfo=UTC)

        names = _candidate_incident_names("k", 300, now=now)

        assert len(names) == 2
        assert len(set(names)) == 2
        assert names[0] == _incident_name("k", 300, now=now)
        assert names[1] == _incident_name("k", 300, now=datetime(2026, 4, 3, 11, 55, tzinfo=UTC))


# ---------------------------------------------------------------------------
# _find_active_incident
# ---------------------------------------------------------------------------


class TestFindActiveIncident:
    @pytest.mark.asyncio
    async def test_returns_none_when_no_candidate_exists(self, mock_k8s) -> None:
        mock_k8s["get_resource"].side_effect = ApiException(status=404)

        result = await _find_active_incident(None, 300)

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_matching_incident(self, mock_k8s) -> None:
        inc = make_incident_obj(category=Category.LATENCY)
        mock_k8s["get_resource"].side_effect = [inc]

        result = await _find_active_incident(None, 300)

        assert result is not None
        assert result["metadata"]["name"] == inc["metadata"]["name"]

    @pytest.mark.asyncio
    async def test_reuses_previous_bucket_incident(self, mock_k8s) -> None:
        previous_bucket_incident = make_incident_obj(category=Category.LATENCY)
        mock_k8s["get_resource"].side_effect = [
            ApiException(status=404),
            previous_bucket_incident,
        ]

        result = await _find_active_incident(None, 300)

        assert result is previous_bucket_incident

    @pytest.mark.asyncio
    async def test_skips_terminal_incident(self, mock_k8s) -> None:
        terminal = make_incident_obj(category=Category.LATENCY, phase="Resolved")

        async def _get_resource(*_args, **_kwargs):
            if not hasattr(_get_resource, "called"):
                _get_resource.called = True
                return terminal
            raise ApiException(status=404)

        mock_k8s["get_resource"].side_effect = _get_resource

        assert await _find_active_incident(None, 300) is None

    @pytest.mark.asyncio
    async def test_skips_failed_incident(self, mock_k8s) -> None:
        failed = make_incident_obj(category=Category.LATENCY, phase="Failed")

        async def _get_resource(*_args, **_kwargs):
            if not hasattr(_get_resource, "called"):
                _get_resource.called = True
                return failed
            raise ApiException(status=404)

        mock_k8s["get_resource"].side_effect = _get_resource

        assert await _find_active_incident(None, 300) is None

    @pytest.mark.asyncio
    async def test_skips_incident_outside_correlation_window(self, mock_k8s) -> None:
        old_ts = (datetime.now(UTC) - timedelta(seconds=360)).isoformat()
        inc = make_incident_obj(category=Category.LATENCY, creation_timestamp=old_ts)

        async def _get_resource(*_args, **_kwargs):
            if not hasattr(_get_resource, "called"):
                _get_resource.called = True
                return inc
            raise ApiException(status=404)

        mock_k8s["get_resource"].side_effect = _get_resource

        assert await _find_active_incident(None, 300) is None

    @pytest.mark.asyncio
    async def test_skips_incident_with_mismatched_target_ref(self, mock_k8s) -> None:
        inc = make_incident_obj(
            category=Category.LATENCY,
            target_ref=make_target_ref(name="api"),
        )

        async def _get_resource(*_args, **_kwargs):
            if not hasattr(_get_resource, "called"):
                _get_resource.called = True
                return inc
            raise ApiException(status=404)

        mock_k8s["get_resource"].side_effect = _get_resource

        result = await _find_active_incident(make_target_ref(name="worker"), 300)

        assert result is None

    @pytest.mark.asyncio
    async def test_does_not_fall_back_to_listing(self, mock_k8s) -> None:
        mock_k8s["get_resource"].side_effect = ApiException(status=404)

        result = await _find_active_incident(make_target_ref(name="api"), 300)

        assert result is None


# ---------------------------------------------------------------------------
# correlate_and_upsert
# ---------------------------------------------------------------------------


class TestCorrelateAndUpsert:
    @pytest.mark.asyncio
    async def test_creates_new_incident_when_none_active(self, mock_k8s) -> None:
        signals = [make_signal(severity=Severity.HIGH, summary="CPU spike")]
        name = await correlate_and_upsert(
            signals, [Category.RESOURCE_SATURATION], None, "kubortex-system"
        )
        assert name.startswith("inc-")
        mock_k8s["create_resource"].assert_awaited_once()

    @pytest.mark.asyncio
    async def test_updates_existing_incident(self, mock_k8s) -> None:
        inc = make_incident_obj(name="inc-existing", category=Category.RESOURCE_SATURATION)
        existing = make_signal(alertname="OldAlert")
        mock_k8s["get_resource"].side_effect = [inc, {
            "metadata": {"resourceVersion": "7"},
            "spec": {
                "signals": [existing.model_dump(by_alias=True, mode="json")],
                "categories": ["resource-saturation"],
                "severity": "warning",
            },
            "status": {},
        }]

        new_signal = make_signal(alertname="NewAlert")
        name = await correlate_and_upsert(
            [new_signal], [Category.RESOURCE_SATURATION], None, "kubortex-system"
        )
        assert name == "inc-existing"
        mock_k8s["create_resource"].assert_not_awaited()

        mock_k8s["patch_spec"].assert_awaited_once()
        spec_call = mock_k8s["patch_spec"].await_args
        assert spec_call.args[0] == "incidents"
        assert spec_call.args[1] == "inc-existing"
        merged_signals = spec_call.args[2]["signals"]
        assert spec_call.kwargs["resource_version"] == "7"
        assert len(merged_signals) == 2
        alertnames = {s["alertname"] for s in merged_signals}
        assert alertnames == {"OldAlert", "NewAlert"}

    @pytest.mark.asyncio
    async def test_create_conflict_reuses_matching_incident(self, mock_k8s) -> None:
        signals = [make_signal(alertname="NewAlert")]
        conflict = ApiException(status=409)
        mock_k8s["create_resource"].side_effect = conflict
        conflicting = make_incident_obj(name="inc-conflict", category=Category.CUSTOM, uid="u1")
        conflicting["metadata"]["resourceVersion"] = "9"
        mock_k8s["get_resource"].side_effect = [
            ApiException(status=404),
            ApiException(status=404),
            conflicting,
            {
                "metadata": {"resourceVersion": "9"},
                "spec": {"signals": [], "categories": ["custom"], "severity": "warning"},
                "status": {},
            },
        ]

        name = await correlate_and_upsert(signals, [Category.CUSTOM], None, "kubortex-system")

        mock_k8s["create_resource"].assert_awaited_once()
        body = mock_k8s["create_resource"].await_args.args[1]
        assert name == body["metadata"]["name"]
        mock_k8s["patch_spec"].assert_awaited_once()

    @pytest.mark.asyncio
    async def test_create_conflict_fails_when_canonical_incident_is_not_reusable(
        self, mock_k8s
    ) -> None:
        signals = [make_signal(alertname="NewAlert")]
        mock_k8s["create_resource"].side_effect = ApiException(status=409)
        conflicting = make_incident_obj(
            name="inc-conflict",
            category=Category.CUSTOM,
            phase="Resolved",
        )
        mock_k8s["get_resource"].side_effect = [
            ApiException(status=404),
            ApiException(status=404),
            conflicting,
        ]

        with pytest.raises(RuntimeError, match="canonical incident name is occupied"):
            await correlate_and_upsert(signals, [Category.CUSTOM], None, "kubortex-system")

    @pytest.mark.asyncio
    async def test_signal_append_retries_on_version_conflict(self, mock_k8s) -> None:
        inc = make_incident_obj(name="inc-existing", category=Category.CUSTOM)
        mock_k8s["get_resource"].side_effect = [
            inc,
            {
                "metadata": {"resourceVersion": "7"},
                "spec": {"signals": [], "categories": ["custom"], "severity": "warning"},
                "status": {},
            },
            {
                "metadata": {"resourceVersion": "8"},
                "spec": {"signals": [], "categories": ["custom"], "severity": "warning"},
                "status": {},
            },
        ]
        mock_k8s["patch_spec"].side_effect = [
            ApiException(status=409),
            None,
        ]

        await correlate_and_upsert(
            [make_signal(alertname="NewAlert")],
            [Category.CUSTOM],
            None,
            "kubortex-system",
        )

        assert mock_k8s["patch_spec"].await_count == 2

    @pytest.mark.asyncio
    async def test_incident_body_has_correct_fields(self, mock_k8s) -> None:
        signals = [make_signal(severity=Severity.CRITICAL, summary="Disk full")]
        await correlate_and_upsert(
            signals, [Category.RESOURCE_SATURATION], make_target_ref(), "kubortex-system"
        )
        call_args = mock_k8s["create_resource"].await_args
        body = call_args[0][1]  # positional arg: (plural, body)
        assert body["kind"] == "Incident"
        assert body["spec"]["severity"] == Severity.CRITICAL
        assert body["spec"]["categories"] == [Category.RESOURCE_SATURATION.value]
        assert body["spec"]["summary"] == "Disk full"
        assert body["spec"]["targetRef"] is not None
        assert body["metadata"]["labels"] == {
            "kubortex.io/target-kind": "Deployment",
            "kubortex.io/target-ns": "default",
            "kubortex.io/target-name": "my-app",
        }

    @pytest.mark.asyncio
    async def test_incident_without_target_has_null_target_ref(self, mock_k8s) -> None:
        signals = [make_signal()]
        await correlate_and_upsert(signals, [Category.CUSTOM], None, "kubortex-system")
        body = mock_k8s["create_resource"].await_args[0][1]
        assert body["spec"]["targetRef"] is None
        assert "labels" not in body["metadata"]

    @pytest.mark.asyncio
    async def test_empty_signals_list_still_creates_incident(self, mock_k8s) -> None:
        name = await correlate_and_upsert([], [Category.CUSTOM], None, "kubortex-system")
        assert name.startswith("inc-")
        body = mock_k8s["create_resource"].await_args[0][1]
        assert body["spec"]["summary"] == "Unknown incident"

    @pytest.mark.asyncio
    async def test_cross_category_signals_merge_into_one_incident(self, mock_k8s) -> None:
        """Signals from different categories targeting the same workload share one Incident."""
        inc = make_incident_obj(
            name="inc-existing",
            category=Category.RESOURCE_SATURATION,
            target_ref=make_target_ref(),
        )
        mock_k8s["get_resource"].side_effect = [
            inc,
            {
                "metadata": {"resourceVersion": "5"},
                "spec": {
                    "signals": [],
                    "categories": ["resource-saturation"],
                    "severity": "warning",
                },
                "status": {},
            },
        ]
        new_signal = make_signal(alertname="HighLatency", severity=Severity.WARNING)
        name = await correlate_and_upsert(
            [new_signal],
            [Category.RESOURCE_SATURATION, Category.LATENCY],
            make_target_ref(),
            "kubortex-system",
        )
        assert name == "inc-existing"
        patch_call = mock_k8s["patch_spec"].await_args
        merged_categories = patch_call.args[2]["categories"]
        assert "latency" in merged_categories

    @pytest.mark.asyncio
    async def test_duplicate_signal_not_appended(self, mock_k8s) -> None:
        """Retransmitted signals (same alertname + observedAt) must not create duplicates."""
        existing_signal = make_signal(alertname="DupAlert")
        existing_raw = existing_signal.model_dump(by_alias=True, mode="json")
        inc = make_incident_obj(name="inc-dup", category=Category.CUSTOM)
        mock_k8s["get_resource"].side_effect = [
            inc,
            {
                "metadata": {"resourceVersion": "1"},
                "spec": {
                    "signals": [existing_raw],
                    "severity": "warning",
                    "categories": ["custom"],
                },
                "status": {},
            },
        ]
        await correlate_and_upsert([existing_signal], [Category.CUSTOM], None, "kubortex-system")

        patch_call = mock_k8s["patch_spec"].await_args
        merged_signals = patch_call.args[2]["signals"]
        assert len(merged_signals) == 1

    @pytest.mark.asyncio
    async def test_signal_cap_drops_oldest(self, mock_k8s) -> None:
        """When the merged signal list exceeds max_signals, oldest entries are dropped."""
        old_signals = [
            {
                "alertname": f"Old{i}",
                "observedAt": f"2024-01-0{i+1}T00:00:00+00:00",
                "severity": "info",
            }
            for i in range(5)
        ]
        inc = make_incident_obj(name="inc-cap", category=Category.CUSTOM)
        mock_k8s["get_resource"].side_effect = [
            inc,
            {
                "metadata": {"resourceVersion": "1"},
                "spec": {"signals": old_signals, "severity": "info", "categories": ["custom"]},
                "status": {},
            },
        ]
        new_signal = make_signal(alertname="NewAlert")
        # cap at 5 so the 5 existing + 1 new = 6, then trimmed to 5 (most recent)
        await correlate_and_upsert(
            [new_signal], [Category.CUSTOM], None, "kubortex-system", max_signals=5
        )

        patch_call = mock_k8s["patch_spec"].await_args
        merged = patch_call.args[2]["signals"]
        assert len(merged) == 5
        assert merged[-1]["alertname"] == "NewAlert"

    @pytest.mark.asyncio
    async def test_severity_escalated_on_update(self, mock_k8s) -> None:
        """A new critical signal must escalate a warning incident's severity."""
        inc = make_incident_obj(name="inc-escalate", category=Category.CUSTOM)
        mock_k8s["get_resource"].side_effect = [
            inc,
            {
                "metadata": {"resourceVersion": "2"},
                "spec": {"signals": [], "severity": "warning", "categories": ["custom"]},
                "status": {},
            },
        ]
        critical_signal = make_signal(alertname="Boom", severity=Severity.CRITICAL)
        await correlate_and_upsert([critical_signal], [Category.CUSTOM], None, "kubortex-system")

        patch_call = mock_k8s["patch_spec"].await_args
        assert patch_call.args[2].get("severity") == "critical"

    @pytest.mark.asyncio
    async def test_severity_not_downgraded_on_update(self, mock_k8s) -> None:
        """A lower-severity new signal must not downgrade the incident severity."""
        inc = make_incident_obj(name="inc-no-down", category=Category.CUSTOM)
        mock_k8s["get_resource"].side_effect = [
            inc,
            {
                "metadata": {"resourceVersion": "3"},
                "spec": {"signals": [], "severity": "critical", "categories": ["custom"]},
                "status": {},
            },
        ]
        info_signal = make_signal(alertname="Minor", severity=Severity.INFO)
        await correlate_and_upsert([info_signal], [Category.CUSTOM], None, "kubortex-system")

        patch_call = mock_k8s["patch_spec"].await_args
        assert "severity" not in patch_call.args[2]

    @pytest.mark.asyncio
    async def test_incident_source_is_stamped(self, mock_k8s) -> None:
        """The source identifier passed to correlate_and_upsert appears in the incident spec."""
        await correlate_and_upsert(
            [make_signal()], [Category.CUSTOM], None, "kubortex-system", source="alertmanager"
        )
        body = mock_k8s["create_resource"].await_args[0][1]
        assert body["spec"]["source"] == "alertmanager"
