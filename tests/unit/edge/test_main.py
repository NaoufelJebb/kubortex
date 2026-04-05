"""Unit tests for kubortex.edge.main (create_app factory + lifespan)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient
from kubernetes_asyncio import config as k8s_config

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


# ---------------------------------------------------------------------------
# _bootstrap_kubernetes — exception chaining
# ---------------------------------------------------------------------------


class TestBootstrapKubernetes:
    @pytest.mark.asyncio
    async def test_chains_exception_when_both_methods_fail(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When both config methods fail the second exception chains the first."""
        in_cluster_exc = k8s_config.ConfigException("not in cluster")
        kube_exc = Exception("no kubeconfig")

        monkeypatch.setattr(
            main_module.k8s_config,
            "load_incluster_config",
            lambda: (_ for _ in ()).throw(in_cluster_exc),
        )

        async def _fail_kubeconfig():
            raise kube_exc

        monkeypatch.setattr(main_module.k8s_config, "load_kube_config", _fail_kubeconfig)

        with pytest.raises(Exception) as exc_info:
            await main_module._bootstrap_kubernetes()

        assert exc_info.value is kube_exc
        assert exc_info.value.__cause__ is in_cluster_exc

    @pytest.mark.asyncio
    async def test_succeeds_with_kubeconfig_when_not_in_cluster(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Fallback to kubeconfig succeeds silently when in-cluster config is absent."""
        monkeypatch.setattr(
            main_module.k8s_config,
            "load_incluster_config",
            lambda: (_ for _ in ()).throw(k8s_config.ConfigException("not in cluster")),
        )

        async def _ok():
            pass

        monkeypatch.setattr(main_module.k8s_config, "load_kube_config", _ok)

        await main_module._bootstrap_kubernetes()  # must not raise

    @pytest.mark.asyncio
    async def test_succeeds_with_incluster_config(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """In-cluster config success skips kubeconfig entirely."""
        monkeypatch.setattr(main_module.k8s_config, "load_incluster_config", lambda: None)

        async def _should_not_be_called():
            raise AssertionError("kubeconfig should not be loaded when in-cluster succeeds")

        monkeypatch.setattr(main_module.k8s_config, "load_kube_config", _should_not_be_called)

        await main_module._bootstrap_kubernetes()  # must not raise
