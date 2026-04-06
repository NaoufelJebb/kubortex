"""Unit tests for kubortex.shared.kube_clients."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from kubortex.shared import kube_clients


@pytest_asyncio.fixture(autouse=True)
async def reset_shared_clients() -> None:
    await kube_clients.close_kubernetes_clients()
    yield
    await kube_clients.close_kubernetes_clients()


class TestGetKubernetesClients:
    @pytest.mark.asyncio
    async def test_initializes_bundle_once(self, monkeypatch: pytest.MonkeyPatch) -> None:
        api_client = SimpleNamespace(close=AsyncMock())
        api_factory = MagicMock(return_value=api_client)
        core_factory = MagicMock(side_effect=lambda api: SimpleNamespace(api=api))
        apps_factory = MagicMock(side_effect=lambda api: SimpleNamespace(api=api))
        custom_objects_factory = MagicMock(side_effect=lambda api: SimpleNamespace(api=api))

        monkeypatch.setattr(kube_clients, "_clients", None)
        monkeypatch.setattr(kube_clients.k8s_client, "ApiClient", api_factory)
        monkeypatch.setattr(kube_clients.k8s_client, "CoreV1Api", core_factory)
        monkeypatch.setattr(kube_clients.k8s_client, "AppsV1Api", apps_factory)
        monkeypatch.setattr(kube_clients.k8s_client, "CustomObjectsApi", custom_objects_factory)

        first = await kube_clients.get_kubernetes_clients()
        second = await kube_clients.get_kubernetes_clients()

        assert first is second
        api_factory.assert_called_once_with()
        core_factory.assert_called_once_with(api_client)
        apps_factory.assert_called_once_with(api_client)
        custom_objects_factory.assert_called_once_with(api_client)

    @pytest.mark.asyncio
    async def test_close_resets_bundle_for_later_reinitialization(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        first_api_client = SimpleNamespace(close=AsyncMock())
        second_api_client = SimpleNamespace(close=AsyncMock())
        api_factory = MagicMock(side_effect=[first_api_client, second_api_client])
        core_factory = MagicMock(side_effect=lambda api: SimpleNamespace(api=api))
        apps_factory = MagicMock(side_effect=lambda api: SimpleNamespace(api=api))
        custom_objects_factory = MagicMock(side_effect=lambda api: SimpleNamespace(api=api))

        monkeypatch.setattr(kube_clients, "_clients", None)
        monkeypatch.setattr(kube_clients.k8s_client, "ApiClient", api_factory)
        monkeypatch.setattr(kube_clients.k8s_client, "CoreV1Api", core_factory)
        monkeypatch.setattr(kube_clients.k8s_client, "AppsV1Api", apps_factory)
        monkeypatch.setattr(kube_clients.k8s_client, "CustomObjectsApi", custom_objects_factory)

        first = await kube_clients.get_kubernetes_clients()
        await kube_clients.close_kubernetes_clients()
        second = await kube_clients.get_kubernetes_clients()

        assert first is not second
        first_api_client.close.assert_awaited_once()
        assert api_factory.call_count == 2
