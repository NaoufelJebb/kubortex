"""Shared Kubernetes API clients for long-lived process reuse.

This module keeps a single process-scoped ``ApiClient`` and the small set of
typed APIs used on hot paths. Callers reuse these clients instead of creating
new sessions for every Kubernetes read.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from kubernetes_asyncio import client as k8s_client


@dataclass(slots=True)
class KubernetesClients:
    """Bundle of long-lived Kubernetes API clients reused within one process."""

    api_client: k8s_client.ApiClient
    core_v1: k8s_client.CoreV1Api
    apps_v1: k8s_client.AppsV1Api
    custom_objects: k8s_client.CustomObjectsApi


_clients: KubernetesClients | None = None
_clients_lock = asyncio.Lock()


async def get_kubernetes_clients() -> KubernetesClients:
    """Return the shared Kubernetes client bundle.

    The bundle is created lazily on first use and reused for the remainder of
    the process lifetime, or until ``close_kubernetes_clients()`` is called.
    """

    global _clients

    if _clients is not None:
        return _clients

    async with _clients_lock:
        if _clients is None:
            api_client = k8s_client.ApiClient()
            _clients = KubernetesClients(
                api_client=api_client,
                core_v1=k8s_client.CoreV1Api(api_client),
                apps_v1=k8s_client.AppsV1Api(api_client),
                custom_objects=k8s_client.CustomObjectsApi(api_client),
            )
        return _clients


async def close_kubernetes_clients() -> None:
    """Close and reset the shared Kubernetes client bundle if initialized."""

    global _clients

    async with _clients_lock:
        if _clients is None:
            return
        api_client = _clients.api_client
        _clients = None

    await api_client.close()
