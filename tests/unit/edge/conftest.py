"""Shared fixtures and factories for kubortex.edge unit tests."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock

import pytest

from kubortex.shared.models.incident import Signal, TargetRef
from kubortex.shared.types import Category, Severity

# ---------------------------------------------------------------------------
# Domain object factories
# ---------------------------------------------------------------------------


def make_signal(
    alertname: str = "TestAlert",
    severity: Severity = Severity.WARNING,
    summary: str = "test summary",
    payload: dict[str, str] | None = None,
    observed_at: datetime | None = None,
) -> Signal:
    return Signal(
        alertname=alertname,
        severity=severity,
        summary=summary,
        observedAt=observed_at or datetime(2024, 1, 1, tzinfo=UTC),
        payload=payload or {},
    )


def make_target_ref(
    kind: str = "Deployment",
    namespace: str = "default",
    name: str = "my-app",
) -> TargetRef:
    return TargetRef(kind=kind, namespace=namespace, name=name)


def make_alert(
    alertname: str = "TestAlert",
    severity: str = "warning",
    namespace: str = "default",
    deployment: str = "my-app",
    status: str = "firing",
    starts_at: str | None = None,
    extra_labels: dict[str, str] | None = None,
    annotations: dict[str, str] | None = None,
) -> dict[str, Any]:
    labels: dict[str, str] = {
        "alertname": alertname,
        "severity": severity,
        "namespace": namespace,
        "deployment": deployment,
    }
    if extra_labels:
        labels.update(extra_labels)
    return {
        "status": status,
        "labels": labels,
        "annotations": {"summary": "Something is wrong"} if annotations is None else annotations,
        "startsAt": starts_at or "2024-01-01T00:00:00Z",
    }


def make_incident_obj(
    name: str = "inc-20240101-aabbccdd",
    namespace: str = "kubortex-system",
    category: Category = Category.RESOURCE_SATURATION,
    phase: str = "Detected",
    creation_timestamp: str = "2099-01-01T00:00:00Z",
    uid: str = "uid-1234",
    target_ref: TargetRef | None = None,
) -> dict[str, Any]:
    """Build a minimal Kubernetes Incident CRD dict."""
    spec: dict[str, Any] = {"signals": []}
    if target_ref is not None:
        spec["targetRef"] = target_ref.model_dump()

    return {
        "metadata": {
            "name": name,
            "namespace": namespace,
            "uid": uid,
            "creationTimestamp": creation_timestamp,
            "labels": {
                "kubortex.io/category": category,
                "kubortex.io/severity": Severity.WARNING,
            },
        },
        "spec": spec,
        "status": {"phase": phase},
    }


# ---------------------------------------------------------------------------
# Pytest fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def signal() -> Signal:
    return make_signal()


@pytest.fixture()
def target_ref() -> TargetRef:
    return make_target_ref()


@pytest.fixture()
def alert() -> dict[str, Any]:
    return make_alert()


@pytest.fixture()
def mock_k8s(monkeypatch) -> dict[str, AsyncMock]:
    """Patch all kubortex.shared.k8s functions used by the correlator."""
    mocks: dict[str, AsyncMock] = {
        "list_resources": AsyncMock(return_value=[]),
        "create_resource": AsyncMock(return_value={}),
        "get_resource": AsyncMock(return_value={"spec": {"signals": []}}),
        "patch_spec": AsyncMock(return_value={}),
        "patch_status": AsyncMock(return_value={}),
    }
    for fn, mock in mocks.items():
        monkeypatch.setattr(f"kubortex.edge.signals.correlator.{fn}", mock)
    return mocks
