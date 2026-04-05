"""Unit tests for kubortex.edge.main (create_app factory + lifespan)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

from fastapi.testclient import TestClient

import kubortex.edge.main as main_module
from kubortex.edge.main import create_app
from kubortex.shared.config import EdgeSettings

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _settings(bot_token: str = "") -> EdgeSettings:
    return EdgeSettings(slack_bot_token=bot_token)


def _make_app(settings: EdgeSettings, monkeypatch):
    """Build a testable app with NotificationRouter.run() stubbed out."""
    mock_router = MagicMock()
    mock_router.run = AsyncMock()
    mock_router.enabled = bool(settings.slack_bot_token)
    mock_router.is_ready = True
    monkeypatch.setattr(main_module, "NotificationRouter", lambda *a, **kw: mock_router)
    monkeypatch.setattr(main_module, "configure_logging", MagicMock())
    return create_app(settings), mock_router


# ---------------------------------------------------------------------------
# create_app — signal ingestion wiring
# ---------------------------------------------------------------------------


class TestSignalIngestion:
    def test_alerts_endpoint_registered(self, monkeypatch) -> None:
        app, _ = _make_app(_settings(), monkeypatch)
        paths = {r.path for r in app.routes}
        assert "/api/v1/alerts" in paths

    def test_healthz_registered(self, monkeypatch) -> None:
        app, _ = _make_app(_settings(), monkeypatch)
        paths = {r.path for r in app.routes}
        assert "/healthz" in paths

    def test_readyz_registered(self, monkeypatch) -> None:
        app, _ = _make_app(_settings(), monkeypatch)
        paths = {r.path for r in app.routes}
        assert "/readyz" in paths


# ---------------------------------------------------------------------------
# create_app — notification router wiring
# ---------------------------------------------------------------------------


class TestNotificationWiring:
    def test_slack_sink_registered_when_token_present(self, monkeypatch) -> None:
        _, mock_router = _make_app(_settings(bot_token="xoxb-test"), monkeypatch)
        mock_router.register.assert_called_once()

    def test_no_sink_registered_when_token_absent(self, monkeypatch) -> None:
        _, mock_router = _make_app(_settings(bot_token=""), monkeypatch)
        mock_router.register.assert_not_called()


# ---------------------------------------------------------------------------
# Lifespan — startup / shutdown
# ---------------------------------------------------------------------------


class TestLifespan:
    def test_notification_router_started_on_startup(self, monkeypatch) -> None:
        app, mock_router = _make_app(_settings(bot_token="xoxb-test"), monkeypatch)
        with TestClient(app):
            pass
        mock_router.run.assert_called_once()

    def test_shutdown_does_not_raise(self, monkeypatch) -> None:
        app, _ = _make_app(_settings(), monkeypatch)
        with TestClient(app):
            pass

    def test_each_create_app_call_is_independent(self, monkeypatch) -> None:
        """Two app instances must not share notification router state."""
        _app1, router1 = _make_app(_settings(bot_token="xoxb-1"), monkeypatch)
        _app2, router2 = _make_app(_settings(bot_token=""), monkeypatch)
        assert router1 is not router2
        router1.register.assert_called_once()
        router2.register.assert_not_called()

    def test_readyz_returns_not_ready_when_router_reports_false(self, monkeypatch) -> None:
        app, mock_router = _make_app(_settings(bot_token="xoxb-test"), monkeypatch)
        mock_router.is_ready = False

        with TestClient(app) as client:
            response = client.get("/readyz")

        assert response.status_code == 503
        assert response.json() == {"status": "not_ready"}
