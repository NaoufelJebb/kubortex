"""Unit tests for kubortex.edge.signals.alertmanager (FastAPI router)."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from kubortex.edge.main import create_app
from kubortex.shared.types import Category

from ..conftest import make_alert, make_signal, make_target_ref

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_payload(*alerts) -> dict:
    return {"alerts": list(alerts)}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def client() -> TestClient:
    return TestClient(create_app(), raise_server_exceptions=True)


@pytest.fixture()
def mock_correlate(monkeypatch) -> AsyncMock:
    mock = AsyncMock(return_value="inc-20240101-aabbccdd")
    monkeypatch.setattr("kubortex.edge.core.ingester.correlate_and_upsert", mock)
    return mock


# ---------------------------------------------------------------------------
# GET /healthz  /readyz
# ---------------------------------------------------------------------------


class TestHealthEndpoints:
    def test_healthz_returns_ok(self, client: TestClient) -> None:
        resp = client.get("/healthz")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}

    def test_readyz_returns_ok(self, client: TestClient) -> None:
        resp = client.get("/readyz")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}


# ---------------------------------------------------------------------------
# POST /api/v1/alerts
# ---------------------------------------------------------------------------


class TestReceiveAlerts:
    def test_empty_alerts_list_returns_200_no_processing(
        self, client: TestClient, mock_correlate: AsyncMock
    ) -> None:
        resp = client.post("/api/v1/alerts", json={"alerts": []})
        assert resp.status_code == 200
        mock_correlate.assert_not_awaited()

    def test_missing_alerts_key_returns_200_no_processing(
        self, client: TestClient, mock_correlate: AsyncMock
    ) -> None:
        resp = client.post("/api/v1/alerts", json={})
        assert resp.status_code == 200
        mock_correlate.assert_not_awaited()

    def test_resolved_alerts_are_skipped(
        self, client: TestClient, mock_correlate: AsyncMock
    ) -> None:
        payload = _make_payload(make_alert(status="resolved"))
        resp = client.post("/api/v1/alerts", json=payload)
        assert resp.status_code == 200
        mock_correlate.assert_not_awaited()

    def test_resolved_alerts_do_not_call_normaliser(self, client: TestClient, monkeypatch) -> None:
        mock_normalise = AsyncMock()
        monkeypatch.setattr("kubortex.edge.signals.alertmanager.normalise_alert", mock_normalise)
        monkeypatch.setattr("kubortex.edge.core.ingester.correlate_and_upsert", AsyncMock())

        resp = client.post("/api/v1/alerts", json=_make_payload(make_alert(status="resolved")))

        assert resp.status_code == 200
        mock_normalise.assert_not_called()

    def test_single_firing_alert_calls_correlate(
        self, client: TestClient, mock_correlate: AsyncMock
    ) -> None:
        payload = _make_payload(make_alert())
        resp = client.post("/api/v1/alerts", json=payload)
        assert resp.status_code == 200
        mock_correlate.assert_awaited_once()

    def test_two_alerts_same_group_produce_one_upsert(
        self, client: TestClient, mock_correlate: AsyncMock
    ) -> None:
        # Same alertname + same deployment → same correlation key → one upsert
        a1 = make_alert(alertname="HighCpu", deployment="api")
        a2 = make_alert(alertname="HighCpu", deployment="api")
        resp = client.post("/api/v1/alerts", json=_make_payload(a1, a2))
        assert resp.status_code == 200
        mock_correlate.assert_awaited_once()

    def test_two_alerts_different_groups_produce_two_upserts(
        self, client: TestClient, mock_correlate: AsyncMock
    ) -> None:
        # Different deployments → different correlation keys → two upserts
        a1 = make_alert(alertname="HighCpu", deployment="api")
        a2 = make_alert(alertname="HighMemory", deployment="worker")
        resp = client.post("/api/v1/alerts", json=_make_payload(a1, a2))
        assert resp.status_code == 200
        assert mock_correlate.await_count == 2

    def test_mixed_firing_and_resolved_only_fires_processed(
        self, client: TestClient, mock_correlate: AsyncMock
    ) -> None:
        firing = make_alert(status="firing")
        resolved = make_alert(status="resolved", deployment="other-app")
        resp = client.post("/api/v1/alerts", json=_make_payload(firing, resolved))
        assert resp.status_code == 200
        mock_correlate.assert_awaited_once()

    def test_response_body_contains_processed_count(
        self, client: TestClient, mock_correlate: AsyncMock
    ) -> None:
        payload = _make_payload(make_alert())
        resp = client.post("/api/v1/alerts", json=payload)
        assert resp.text == "processed 1 signals"

    def test_invalid_alert_shape_returns_400(self, client: TestClient, monkeypatch) -> None:
        monkeypatch.setattr("kubortex.edge.core.ingester.correlate_and_upsert", AsyncMock())

        resp = client.post("/api/v1/alerts", json={"alerts": ["bad"]})

        assert resp.status_code == 400
        assert resp.json() == {"detail": "each alert must be a JSON object"}

    def test_invalid_timestamp_returns_400(self, client: TestClient, monkeypatch) -> None:
        monkeypatch.setattr("kubortex.edge.core.ingester.correlate_and_upsert", AsyncMock())

        resp = client.post(
            "/api/v1/alerts",
            json=_make_payload(make_alert(starts_at="not-a-timestamp")),
        )

        assert resp.status_code == 400
        assert resp.json() == {"detail": "alert.startsAt must be an ISO 8601 timestamp"}

    def test_groups_signals_and_forwards_namespace_to_correlator(
        self, client: TestClient, monkeypatch
    ) -> None:
        signal_one = make_signal(summary="CPU high")
        signal_two = make_signal(summary="CPU still high")
        target = make_target_ref(name="api")

        mock_normalise = AsyncMock(
            side_effect=[
                (signal_one, Category.RESOURCE_SATURATION, target),
                (signal_two, Category.RESOURCE_SATURATION, target),
            ]
        )
        mock_correlate = AsyncMock(return_value="inc-20240101-aabbccdd")

        monkeypatch.setattr("kubortex.edge.signals.alertmanager.normalise_alert", mock_normalise)
        monkeypatch.setattr(
            "kubortex.edge.core.ingester.correlate_and_upsert",
            mock_correlate,
        )

        resp = client.post("/api/v1/alerts", json=_make_payload(make_alert(), make_alert()))

        assert resp.status_code == 200
        mock_correlate.assert_awaited_once()
        args = mock_correlate.await_args.args
        assert args[0] == [signal_one, signal_two]
        assert args[1] == [Category.RESOURCE_SATURATION]
        assert args[2] == target
        assert args[3] == "kubortex-system"

    def test_request_path_does_not_preload_incident_index(
        self, client: TestClient, monkeypatch
    ) -> None:
        mock_correlate = AsyncMock(return_value="inc-20240101-aabbccdd")

        monkeypatch.setattr("kubortex.edge.core.ingester.correlate_and_upsert", mock_correlate)

        resp = client.post("/api/v1/alerts", json=_make_payload(make_alert(), make_alert()))

        assert resp.status_code == 200
        mock_correlate.assert_awaited_once()
