"""Entry point for kubortex-edge — FastAPI application."""

from __future__ import annotations

import uvicorn
from fastapi import FastAPI

from kubortex.edge.signals.alertmanager import router as alerts_router
from kubortex.shared.logging import configure_logging

app = FastAPI(title="kubortex-edge", version="0.4.0-alpha")
app.include_router(alerts_router)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/readyz")
async def readyz() -> dict[str, str]:
    return {"status": "ok"}


@app.on_event("startup")
async def startup() -> None:
    configure_logging(component="edge")


def main() -> None:
    uvicorn.run("kubortex.edge.main:app", host="0.0.0.0", port=8080, log_level="info")


if __name__ == "__main__":
    main()
