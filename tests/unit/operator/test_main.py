"""Unit tests for kubortex.operator.main."""

from __future__ import annotations

from unittest.mock import MagicMock

from kubortex.operator.main import login, on_startup
from kubortex.operator.settings import settings


class TestOnStartup:
    async def test_configures_logging_with_correct_args(self, monkeypatch) -> None:
        mock_log = MagicMock()
        monkeypatch.setattr("kubortex.operator.main.configure_logging", mock_log)
        await on_startup()
        mock_log.assert_called_once_with(component="operator", level=settings.log_level)


class TestLogin:
    async def test_delegates_to_kopf_login_via_client(self, monkeypatch) -> None:
        mock_login = MagicMock(return_value="connection-info")
        monkeypatch.setattr("kopf.login_via_client", mock_login)
        result = await login(server="https://kube.example.com")
        mock_login.assert_called_once_with(server="https://kube.example.com")
        assert result == "connection-info"

    async def test_forwards_all_kwargs(self, monkeypatch) -> None:
        mock_login = MagicMock(return_value=None)
        monkeypatch.setattr("kopf.login_via_client", mock_login)
        await login(a=1, b=2)
        mock_login.assert_called_once_with(a=1, b=2)
