"""Unit tests for kubortex.edge.signals.normaliser."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from kubortex.edge.signals.normaliser import (
    extract_target_hints,
    infer_category,
    normalise_alert,
    normalise_severity,
)
from kubortex.shared.types import Category, Severity

from ..conftest import make_alert, make_target_ref


class TestNormaliseSeverity:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("critical", Severity.CRITICAL),
            ("CRITICAL", Severity.CRITICAL),
            ("error", Severity.HIGH),
            ("high", Severity.HIGH),
            ("warning", Severity.WARNING),
            ("info", Severity.INFO),
            ("none", Severity.INFO),
        ],
    )
    def test_known_severities(self, raw: str, expected: Severity) -> None:
        assert normalise_severity(raw) == expected

    def test_unknown_severity_defaults_to_warning(self) -> None:
        assert normalise_severity("bogus") == Severity.WARNING

    def test_empty_string_defaults_to_warning(self) -> None:
        assert normalise_severity("") == Severity.WARNING


class TestInferCategory:
    @pytest.mark.parametrize(
        ("alertname", "expected"),
        [
            ("HighCpuUsage", Category.RESOURCE_SATURATION),
            ("MemoryPressure", Category.RESOURCE_SATURATION),
            ("OOMKiller", Category.RESOURCE_SATURATION),
            ("DiskFull", Category.RESOURCE_SATURATION),
            ("ErrorRateHigh", Category.ERROR_RATE),
            ("5xxErrors", Category.ERROR_RATE),
            ("LatencyP99High", Category.LATENCY),
            ("ResponseTimeTooLong", Category.LATENCY),
            ("ServiceDown", Category.AVAILABILITY),
            ("PodUnavailable", Category.AVAILABILITY),
            ("DeploymentFailed", Category.DEPLOYMENT),
            ("RolloutBlocked", Category.DEPLOYMENT),
            ("SecurityViolation", Category.SECURITY),
            ("CapacityLow", Category.CAPACITY),
        ],
    )
    def test_keyword_inference(self, alertname: str, expected: Category) -> None:
        assert infer_category(alertname, {}) == expected

    def test_unknown_alertname_returns_custom(self) -> None:
        assert infer_category("SomethingWeird", {}) == Category.CUSTOM

    def test_explicit_kubortex_category_label_wins(self) -> None:
        labels = {"kubortex_category": "latency"}
        assert infer_category("HighCpuUsage", labels) == Category.LATENCY

    def test_explicit_kubortex_io_category_label_wins(self) -> None:
        labels = {"kubortex.io/category": "latency"}
        assert infer_category("HighCpuUsage", labels) == Category.LATENCY

    def test_explicit_category_label_wins(self) -> None:
        labels = {"category": "security"}
        assert infer_category("HighCpuUsage", labels) == Category.SECURITY

    def test_invalid_explicit_category_falls_back_to_keyword(self) -> None:
        labels = {"kubortex_category": "not-a-valid-category"}
        assert infer_category("HighCpuUsage", labels) == Category.RESOURCE_SATURATION

    def test_invalid_explicit_category_with_no_keyword_match_returns_custom(self) -> None:
        labels = {"category": "garbage"}
        assert infer_category("SomethingWeird", labels) == Category.CUSTOM

    def test_invalid_explicit_category_logs_warning(self) -> None:
        """Invalid category label triggers a structured warning log."""
        import structlog.testing

        with structlog.testing.capture_logs() as logs:
            result = infer_category("HighCpuUsage", {"kubortex_category": "not-valid"})

        assert result == Category.RESOURCE_SATURATION
        assert any(
            log.get("event") == "invalid_category_label" and log.get("value") == "not-valid"
            for log in logs
        )


class TestExtractTargetHints:
    def test_extracts_workload_and_namespace_hints(self) -> None:
        hints = extract_target_hints({"namespace": "prod", "deployment": "api"})
        assert hints.namespace == "prod"
        assert hints.deployment == "api"

    def test_extracts_common_aliases(self) -> None:
        hints = extract_target_hints(
            {
                "exported_namespace": "prod",
                "kubernetes_pod_name": "api-123",
                "kubernetes_service_name": "api-svc",
                "kubernetes_node": "worker-1",
                "kubernetes_persistentvolumeclaim_name": "data-api-0",
            }
        )
        assert hints.namespace == "prod"
        assert hints.pod == "api-123"
        assert hints.service == "api-svc"
        assert hints.node == "worker-1"
        assert hints.pvc == "data-api-0"


class TestNormaliseAlert:
    @pytest.mark.asyncio
    async def test_basic_alert_returns_signal_category_target(self) -> None:
        signal, category, target = await normalise_alert(make_alert())
        assert signal.alertname == "TestAlert"
        assert signal.severity == Severity.WARNING
        assert category == Category.CUSTOM
        assert target is not None
        assert target.kind == "Deployment"

    @pytest.mark.asyncio
    async def test_missing_labels_uses_defaults(self) -> None:
        signal, _, target = await normalise_alert({})
        assert signal.alertname == "UnknownAlert"
        assert signal.severity == Severity.WARNING
        assert target is None

    @pytest.mark.asyncio
    async def test_summary_annotation_used_as_summary(self) -> None:
        signal, _, _ = await normalise_alert(make_alert(annotations={"summary": "High CPU on pod"}))
        assert signal.summary == "High CPU on pod"

    @pytest.mark.asyncio
    async def test_description_annotation_fallback(self) -> None:
        signal, _, _ = await normalise_alert(
            make_alert(annotations={"description": "Fallback description"})
        )
        assert signal.summary == "Fallback description"

    @pytest.mark.asyncio
    async def test_alertname_used_when_no_annotation(self) -> None:
        a = {
            "status": "firing",
            "labels": {
                "alertname": "TestAlert",
                "severity": "warning",
                "namespace": "default",
                "deployment": "my-app",
            },
        }
        signal, _, _ = await normalise_alert(a)
        assert signal.summary == a["labels"]["alertname"]

    @pytest.mark.asyncio
    async def test_starts_at_parsed_correctly(self) -> None:
        signal, _, _ = await normalise_alert(make_alert(starts_at="2024-06-15T12:00:00Z"))
        assert signal.observed_at == datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)

    @pytest.mark.asyncio
    async def test_missing_starts_at_uses_now(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fixed = datetime(2024, 1, 1, tzinfo=UTC)
        monkeypatch.setattr(
            "kubortex.edge.signals.normaliser.datetime",
            type(
                "dt",
                (),
                {
                    "now": staticmethod(lambda tz=None: fixed),
                    "fromisoformat": datetime.fromisoformat,
                },
            )(),
        )
        a = make_alert()
        del a["startsAt"]
        signal, _, _ = await normalise_alert(a)
        assert signal.observed_at == fixed

    @pytest.mark.asyncio
    async def test_value_annotation_stored_in_payload(self) -> None:
        signal, _, _ = await normalise_alert(
            make_alert(annotations={"value": "95.3", "summary": "CPU high"})
        )
        assert signal.payload.get("value") == "95.3"

    @pytest.mark.asyncio
    async def test_value_label_stored_in_payload_when_no_annotation(self) -> None:
        signal, _, _ = await normalise_alert(make_alert(extra_labels={"value": "42"}))
        assert signal.payload.get("value") == "42"

    @pytest.mark.asyncio
    async def test_no_value_gives_empty_payload(self) -> None:
        signal, _, _ = await normalise_alert(make_alert(annotations={"summary": "ok"}))
        assert signal.payload == {}

    @pytest.mark.asyncio
    async def test_invalid_starts_at_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match=r"alert\.startsAt must be an ISO 8601 timestamp"):
            await normalise_alert(make_alert(starts_at="not-a-timestamp"))

    @pytest.mark.asyncio
    async def test_non_mapping_labels_raise_value_error(self) -> None:
        with pytest.raises(ValueError, match=r"alert\.labels must be a JSON object"):
            await normalise_alert({"labels": []})

    @pytest.mark.asyncio
    async def test_non_mapping_annotations_raise_value_error(self) -> None:
        with pytest.raises(ValueError, match=r"alert\.annotations must be a JSON object"):
            await normalise_alert({"annotations": []})

    @pytest.mark.asyncio
    async def test_resolver_is_used_for_target_resolution(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock = AsyncMock(return_value=make_target_ref(kind="Service", name="payments"))
        monkeypatch.setattr("kubortex.edge.signals.normaliser.resolve_target", mock)

        _, _, target = await normalise_alert(
            make_alert(deployment="", extra_labels={"service": "payments"})
        )

        assert target is not None
        assert target.kind == "Service"
        mock.assert_awaited_once()
