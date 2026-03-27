"""Structured logging configuration using structlog with JSON output."""

from __future__ import annotations

import logging

import structlog


def configure_logging(
    *, component: str, level: str = "INFO", json_output: bool = True
) -> None:
    """Set up structlog processors for a Kubortex component.

    Args:
        component: Name bound to every log line (e.g. ``operator``, ``edge``).
        level: Stdlib logging level (e.g. ``"INFO"``, ``"DEBUG"``).
        json_output: Emit JSON lines when *True*, human-readable otherwise.
    """
    logging.basicConfig(level=level.upper())
    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    renderer: structlog.types.Processor = (
        structlog.processors.JSONRenderer() if json_output else structlog.dev.ConsoleRenderer()
    )

    structlog.configure(
        processors=[
            *shared_processors,
            renderer,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # Bind the component name so it appears on every log line.
    structlog.contextvars.bind_contextvars(component=component)


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """Return a structlog logger, optionally bound to *name*."""
    return structlog.get_logger(name)
