"""Kubortex Remediator entry point.

Initialises the Kubernetes client and starts the worker polling loop.
"""

from __future__ import annotations

import asyncio

from kubernetes_asyncio import config as k8s_config

from kubortex.shared.config import RemediatorSettings
from kubortex.shared.logging import configure_logging

from .worker import RemediatorWorker


async def _run() -> None:
    configure_logging("remediator")

    try:
        k8s_config.load_incluster_config()
    except k8s_config.ConfigException:
        await k8s_config.load_kube_config()

    settings = RemediatorSettings()
    worker = RemediatorWorker(settings)
    await worker.run()


def main() -> None:
    """Sync entry point for ``kubortex-remediator``."""
    asyncio.run(_run())


if __name__ == "__main__":
    main()
