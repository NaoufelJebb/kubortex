"""Unit tests for kubortex.edge.signals.normaliser."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from kubortex.edge.signals.normaliser import (
    extract_target_ref,
    infer_category,
    normalise_alert,
    normalise_severity,
)
from kubortex.shared.types import Category, Severity

from ..conftest import make_alert

# ---------------------------------------------------------------------------
# normalise_severity
# ---------------------------------------------------------------------------


class TestNormaliseSeverity:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("critical", Severity.CRITICAL),
            ("CRITICAL", Severity.CRITICAL),  # case-insensitive
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


# ---------------------------------------------------------------------------
# infer_category
# ---------------------------------------------------------------------------


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

    def test_explicit_category_label_wins(self) -> None:
        labels = {"category": "security"}
        assert infer_category("HighCpuUsage", labels) == Category.SECURITY

    def test_invalid_explicit_category_falls_back_to_keyword(self) -> None:
        labels = {"kubortex_category": "not-a-valid-category"}
        assert infer_category("HighCpuUsage", labels) == Category.RESOURCE_SATURATION

    def test_invalid_explicit_category_with_no_keyword_match_returns_custom(self) -> None:
        labels = {"category": "garbage"}
        assert infer_category("SomethingWeird", labels) == Category.CUSTOM


# ---------------------------------------------------------------------------
# extract_target_ref
# ---------------------------------------------------------------------------


class TestExtractTargetRef:
    def test_deployment_label_produces_deployment_ref(self) -> None:
        labels = {"namespace": "prod", "deployment": "api-server"}
        ref = extract_target_ref(labels)
        assert ref is not None
        assert ref.kind == "Deployment"
        assert ref.namespace == "prod"
        assert ref.name == "api-server"

    def test_statefulset_label_produces_statefulset_ref(self) -> None:
        labels = {"namespace": "prod", "statefulset": "postgres"}
        ref = extract_target_ref(labels)
        assert ref is not None
        assert ref.kind == "StatefulSet"
        assert ref.name == "postgres"

    def test_daemonset_label_produces_daemonset_ref(self) -> None:
        labels = {"namespace": "kube-system", "daemonset": "node-agent"}
        ref = extract_target_ref(labels)
        assert ref is not None
        assert ref.kind == "DaemonSet"

    def test_pod_label_fallback(self) -> None:
        labels = {"namespace": "default", "pod": "api-abc123"}
        ref = extract_target_ref(labels)
        assert ref is not None
        assert ref.kind == "Pod"
        assert ref.name == "api-abc123"

    def test_no_usable_labels_returns_none(self) -> None:
        assert extract_target_ref({}) is None

    def test_workload_name_without_namespace_returns_none(self) -> None:
        # deployment present but no namespace → no match
        assert extract_target_ref({"deployment": "api"}) is None

    def test_deployment_takes_priority_over_pod(self) -> None:
        labels = {"namespace": "default", "deployment": "api", "pod": "api-xyz"}
        ref = extract_target_ref(labels)
        assert ref is not None
        assert ref.kind == "Deployment"


# ---------------------------------------------------------------------------
# normalise_alert (integration of the three helpers)
# ---------------------------------------------------------------------------


class TestNormaliseAlert:
    def test_basic_alert_returns_signal_category_target(self, alert: dict) -> None:
        signal, category, target = normalise_alert(alert)
        assert signal.alertname == "TestAlert"
        assert signal.severity == Severity.WARNING
        assert category == Category.CUSTOM  # "TestAlert" hits no keyword
        assert target is not None
        assert target.kind == "Deployment"

    def test_missing_labels_uses_defaults(self) -> None:
        signal, _, target = normalise_alert({})
        assert signal.alertname == "UnknownAlert"
        assert signal.severity == Severity.WARNING
        assert target is None

    def test_summary_annotation_used_as_summary(self) -> None:
        a = make_alert(annotations={"summary": "High CPU on pod"})
        signal, _, _ = normalise_alert(a)
        assert signal.summary == "High CPU on pod"

    def test_description_annotation_fallback(self) -> None:
        a = make_alert(annotations={"description": "Fallback description"})
        signal, _, _ = normalise_alert(a)
        assert signal.summary == "Fallback description"

    def test_alertname_used_when_no_annotation(self) -> None:
        a = {
            "status": "firing",
            "labels": {
                "alertname": "TestAlert",
                "severity": "warning",
                "namespace": "default",
                "deployment": "my-app",
            },
        }
        signal, _, _ = normalise_alert(a)
        assert signal.summary == a["labels"]["alertname"]

    def test_starts_at_parsed_correctly(self) -> None:
        a = make_alert(starts_at="2024-06-15T12:00:00Z")
        signal, _, _ = normalise_alert(a)
        assert signal.observed_at == datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)

    def test_missing_starts_at_uses_now(self, monkeypatch: pytest.MonkeyPatch) -> None:
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
        signal, _, _ = normalise_alert(a)
        assert signal.observed_at == fixed

    def test_value_annotation_stored_in_payload(self) -> None:
        a = make_alert(annotations={"value": "95.3", "summary": "CPU high"})
        signal, _, _ = normalise_alert(a)
        assert signal.payload.get("value") == "95.3"

    def test_value_label_stored_in_payload_when_no_annotation(self) -> None:
        a = make_alert(extra_labels={"value": "42"})
        signal, _, _ = normalise_alert(a)
        assert signal.payload.get("value") == "42"

    def test_no_value_gives_empty_payload(self) -> None:
        a = make_alert(annotations={"summary": "ok"})
        signal, _, _ = normalise_alert(a)
        assert signal.payload == {}
