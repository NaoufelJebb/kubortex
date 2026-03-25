"""Entry point for kubortex-operator.

Starts kopf with leader election so only one replica is active.
"""

from __future__ import annotations

import kopf

# AIDEV-NOTE: Importing handlers package registers all kopf decorators.
import kubortex.operator.handlers  # noqa: F401
from kubortex.shared.logging import configure_logging


@kopf.on.startup()
async def on_startup(**_: object) -> None:
    configure_logging(component="operator")


@kopf.on.login()
async def login(**kwargs: object) -> kopf.ConnectionInfo:
    """Use in-cluster or kubeconfig credentials."""
    return kopf.login_via_client(**kwargs)


def main() -> None:
    """Run the operator with leader election."""
    kopf.run(
        clusterwide=True,
        liveness_endpoint="http://0.0.0.0:8080/healthz",
    )


if __name__ == "__main__":
    main()
