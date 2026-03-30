"""Entry point for kubortex-operator.

Starts kopf with leader election so only one replica is active.
"""

from __future__ import annotations

import kopf

# NOTE: Importing handlers registers the kopf decorators.
import kubortex.operator.handlers  # noqa: F401
from kubortex.operator.settings import settings
from kubortex.shared.logging import configure_logging


@kopf.on.startup()
async def on_startup(**_: object) -> None:
    configure_logging(component="operator", level=settings.log_level)


@kopf.on.login()
async def login(**kwargs: object) -> kopf.ConnectionInfo:
    """Load Kubernetes credentials for kopf.

    Args:
        **kwargs: Login arguments forwarded by kopf.

    Returns:
        Connection information for the Kubernetes client.
    """
    return kopf.login_via_client(**kwargs)


def main() -> None:
    """Run the operator."""
    kopf.run(  # pragma: no cover
        clusterwide=True,
        liveness_endpoint=f"http://{settings.liveness_host}:{settings.liveness_port}/healthz",
    )


if __name__ == "__main__":  # pragma: no cover
    main()
