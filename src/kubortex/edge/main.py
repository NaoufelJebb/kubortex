"""Entry point for kubortex-edge — FastAPI application."""

from __future__ import annotations

import asyncio
import contextlib

import uvicorn
from fastapi import FastAPI

from kubortex.edge.notifications.router import NotificationRouter
from kubortex.edge.notifications.slack import SlackNotifier
from kubortex.edge.signals.alertmanager import router as alerts_router
from kubortex.shared.config import KubortexSettings
from kubortex.shared.logging import configure_logging

app = FastAPI(title="kubortex-edge", version="0.4.0-alpha")
app.include_router(alerts_router)

_notification_task: asyncio.Task | None = None


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/readyz")
async def readyz() -> dict[str, str]:
    return {"status": "ok"}


@app.on_event("startup")
async def startup() -> None:
    global _notification_task
    configure_logging(component="edge")
    settings = KubortexSettings()
    router = NotificationRouter(settings)
    if settings.slack_bot_token:
        router.register(SlackNotifier(settings))
    _notification_task = asyncio.create_task(router.run())


@app.on_event("shutdown")
async def shutdown() -> None:
    if _notification_task:
        _notification_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await _notification_task


def main() -> None:
    uvicorn.run("kubortex.edge.main:app", host="0.0.0.0", port=8080, log_level="info")


if __name__ == "__main__":
    main()
