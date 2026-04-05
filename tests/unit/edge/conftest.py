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
    spec: dict[str, Any] = {"signals": [], "categories": [category.value], "severity": "warning"}
    if target_ref is not None:
        spec["targetRef"] = target_ref.model_dump()

    labels: dict[str, Any] = {
        "kubortex.io/category": category,
        "kubortex.io/severity": Severity.WARNING,
        "kubortex.io/target-kind": target_ref.kind if target_ref else "",
        "kubortex.io/target-ns": target_ref.namespace if target_ref else "",
        "kubortex.io/target-name": target_ref.name if target_ref else "",
    }

    return {
        "metadata": {
            "name": name,
            "namespace": namespace,
            "uid": uid,
            "creationTimestamp": creation_timestamp,
            "labels": labels,
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
    """Patch shared CRD helpers used by the Edge correlator."""
    mocks: dict[str, AsyncMock] = {
        "create_resource": AsyncMock(return_value={}),
        "get_resource": AsyncMock(return_value={"spec": {"signals": []}, "metadata": {}}),
        "patch_spec": AsyncMock(return_value=None),
    }
    monkeypatch.setattr(
        "kubortex.edge.core.correlator.create_resource",
        mocks["create_resource"],
    )
    monkeypatch.setattr("kubortex.edge.core.correlator.get_resource", mocks["get_resource"])
    monkeypatch.setattr(
        "kubortex.edge.core.correlator.patch_spec",
        mocks["patch_spec"],
    )
    return mocks
