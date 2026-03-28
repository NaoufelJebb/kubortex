"""Unit tests for kubortex.edge.signals.ingester (SignalSource Protocol + SignalIngester)."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient

from kubortex.edge.signals.alertmanager import AlertmanagerSource
from kubortex.edge.signals.ingester import SignalIngester, SignalSource
from kubortex.shared.config import EdgeSettings
from kubortex.shared.models.incident import Signal, TargetRef
from kubortex.shared.types import Category, Severity

from ..conftest import make_signal, make_target_ref

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SETTINGS = EdgeSettings(namespace="test-ns")


class _FakeSource:
    """Minimal SignalSource implementation for testing."""

    path: str = "/api/v1/fake"

    def __init__(self, signals: list[tuple[Signal, Category, TargetRef | None]] | None = None):
        self._signals = signals or []

    async def parse(
        self, payload: dict[str, Any]
    ) -> list[tuple[Signal, Category, TargetRef | None]]:
        return self._signals


def _make_parsed(
    n: int = 1,
    category: Category = Category.RESOURCE_SATURATION,
    target: TargetRef | None = None,
) -> list[tuple[Signal, Category, TargetRef | None]]:
    t = target or make_target_ref()
    return [(make_signal(), category, t) for _ in range(n)]


# ---------------------------------------------------------------------------
# SignalSource Protocol
# ---------------------------------------------------------------------------


class TestSignalSourceProtocol:
    def test_fake_source_satisfies_protocol(self) -> None:
        assert isinstance(_FakeSource(), SignalSource)

    def test_alertmanager_source_satisfies_protocol(self) -> None:
        assert isinstance(AlertmanagerSource(), SignalSource)

    def test_object_without_parse_does_not_satisfy_protocol(self) -> None:
        class _NoMethod:
            path: str = "/x"

        assert not isinstance(_NoMethod(), SignalSource)


# ---------------------------------------------------------------------------
# SignalIngester.register
# ---------------------------------------------------------------------------


class TestRegister:
    def test_register_adds_source(self) -> None:
        ingester = SignalIngester(_SETTINGS)
        src = _FakeSource()
        ingester.register(src)
        assert src in ingester._sources

    def test_register_creates_route(self) -> None:
        ingester = SignalIngester(_SETTINGS)
        ingester.register(_FakeSource())
        routes = [r.path for r in ingester.router.routes]
        assert "/api/v1/fake" in routes

    def test_register_multiple_sources_creates_multiple_routes(self) -> None:
        class _AnotherSource:
            path: str = "/api/v1/other"

            async def parse(self, payload):
                return []

        ingester = SignalIngester(_SETTINGS)
        ingester.register(_FakeSource())
        ingester.register(_AnotherSource())
        routes = {r.path for r in ingester.router.routes}
        assert "/api/v1/fake" in routes
        assert "/api/v1/other" in routes

    def test_constructor_default_settings(self) -> None:
        ingester = SignalIngester()
        assert ingester._settings is not None
        assert ingester._sources == []


# ---------------------------------------------------------------------------
# SignalIngester HTTP handler — via TestClient
# ---------------------------------------------------------------------------


@pytest.fixture()
def _mock_correlate(monkeypatch) -> AsyncMock:
    mock = AsyncMock(return_value="inc-test-001")
    monkeypatch.setattr("kubortex.edge.signals.ingester.correlate_and_upsert", mock)
    return mock


class TestHandler:
    def _client(self, source=None) -> TestClient:
        from fastapi import FastAPI

        ingester = SignalIngester(_SETTINGS)
        ingester.register(source or _FakeSource())
        app = FastAPI()
        app.include_router(ingester.router)
        return TestClient(app)

    def test_empty_parse_result_returns_no_signals(self, _mock_correlate) -> None:
        client = self._client(_FakeSource(signals=[]))
        resp = client.post("/api/v1/fake", json={})
        assert resp.status_code == 200
        assert resp.text == "no signals"
        _mock_correlate.assert_not_awaited()

    def test_single_signal_calls_correlate_once(self, _mock_correlate) -> None:
        parsed = _make_parsed(n=1)
        client = self._client(_FakeSource(signals=parsed))
        resp = client.post("/api/v1/fake", json={})
        assert resp.status_code == 200
        _mock_correlate.assert_awaited_once()

    def test_response_contains_signal_count(self, _mock_correlate) -> None:
        parsed = _make_parsed(n=3, target=make_target_ref(name="app"))
        client = self._client(_FakeSource(signals=parsed))
        resp = client.post("/api/v1/fake", json={})
        assert resp.text == "processed 3 signals"

    def test_signals_same_group_produce_one_correlate_call(self, _mock_correlate) -> None:
        target = make_target_ref(name="api")
        parsed = _make_parsed(n=2, target=target)
        client = self._client(_FakeSource(signals=parsed))
        client.post("/api/v1/fake", json={})
        _mock_correlate.assert_awaited_once()
        args = _mock_correlate.await_args.args
        assert len(args[0]) == 2  # both signals in one group

    def test_signals_different_groups_produce_multiple_correlate_calls(
        self, _mock_correlate
    ) -> None:
        t1 = make_target_ref(name="api")
        t2 = make_target_ref(name="worker")
        parsed = [
            (make_signal(), Category.RESOURCE_SATURATION, t1),
            (make_signal(), Category.RESOURCE_SATURATION, t2),
        ]
        client = self._client(_FakeSource(signals=parsed))
        client.post("/api/v1/fake", json={})
        assert _mock_correlate.await_count == 2

    def test_namespace_forwarded_to_correlate(self, _mock_correlate) -> None:
        parsed = _make_parsed(n=1)
        client = self._client(_FakeSource(signals=parsed))
        client.post("/api/v1/fake", json={})
        args = _mock_correlate.await_args.args
        assert args[3] == "test-ns"

    def test_unknown_target_uses_unknown_key(self, _mock_correlate) -> None:
        parsed = [(make_signal(), Category.RESOURCE_SATURATION, None)]
        client = self._client(_FakeSource(signals=parsed))
        resp = client.post("/api/v1/fake", json={})
        assert resp.status_code == 200
        _mock_correlate.assert_awaited_once()
