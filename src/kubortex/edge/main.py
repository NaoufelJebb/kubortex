"""Entry point for kubortex-edge — FastAPI application."""

from __future__ import annotations

import asyncio
import contextlib
from contextlib import asynccontextmanager
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as package_version

import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from kubernetes_asyncio import config as k8s_config

from kubortex.edge.core.ingester import SignalIngester
from kubortex.edge.core.router import NotificationRouter
from kubortex.edge.notifications.slack import SlackNotifier
from kubortex.edge.signals.alertmanager import AlertmanagerSource
from kubortex.shared.config import EdgeSettings
from kubortex.shared.logging import configure_logging


def _app_version() -> str:
    try:
        return package_version("kubortex")
    except PackageNotFoundError:  # pragma: no cover
        return "0.0.0+unknown"


def create_app(settings: EdgeSettings | None = None) -> FastAPI:
    """Create the edge FastAPI application.

    Args:
        settings: Optional edge settings override.

    Returns:
        Configured FastAPI application.
    """
    s = settings or EdgeSettings()

    ingester = SignalIngester(s)
    ingester.register(AlertmanagerSource())

    notification_router = NotificationRouter(s)
    if s.slack_bot_token:
        notification_router.register(SlackNotifier(s))

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        configure_logging(component="edge", level=s.log_level)
        task = asyncio.create_task(notification_router.run())
        yield
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    _app = FastAPI(title="kubortex-edge", version=_app_version(), lifespan=lifespan)
    _app.include_router(ingester.router)

    @_app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @_app.get("/readyz", response_model=None)
    async def readyz():
        if not notification_router.is_ready:
            return JSONResponse(status_code=503, content={"status": "not_ready"})
        return {"status": "ok"}

    return _app


async def _bootstrap_kubernetes() -> None:
    """Load Kubernetes client configuration from the environment.

    Attempts in-cluster configuration first, then falls back to the local
    kubeconfig file. If both methods fail, raises the second exception with
    the original ``ConfigException`` chained as its cause.

    Raises:
        Exception: If both in-cluster and kubeconfig loading fail.
    """
    try:
        k8s_config.load_incluster_config()
    except k8s_config.ConfigException as exc:
        try:
            await k8s_config.load_kube_config()
        except Exception as kube_exc:
            raise kube_exc from exc


def main() -> None:  # pragma: no cover
    asyncio.run(_bootstrap_kubernetes())
    s = EdgeSettings()
    uvicorn.run(create_app(s), host=s.host, port=s.port, log_level=s.log_level)


if __name__ == "__main__":  # pragma: no cover
    main()
