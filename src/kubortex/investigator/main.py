"""Kubortex Investigator entry point.

Initialises registries and the Kubernetes client, then starts the
worker polling loop.
"""

from __future__ import annotations

import asyncio

from kubernetes_asyncio import config as k8s_config

from kubortex.shared.config import InvestigatorSettings
from kubortex.shared.kube_clients import close_kubernetes_clients
from kubortex.shared.logging import configure_logging

from .worker import InvestigatorWorker


async def _run() -> None:
    configure_logging("investigator")

    # Load in-cluster or kubeconfig
    try:
        k8s_config.load_incluster_config()
    except k8s_config.ConfigException:
        await k8s_config.load_kube_config()

    settings = InvestigatorSettings()
    worker = InvestigatorWorker(settings)
    try:
        await worker.run()
    finally:
        await close_kubernetes_clients()


def main() -> None:
    """Sync entry point for ``kubortex-investigator``."""
    asyncio.run(_run())


if __name__ == "__main__":
    main()
