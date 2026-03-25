"""Action registry — maps action type strings to action implementations."""

from __future__ import annotations

from kubortex.remediator.actions.base import BaseAction
from kubortex.remediator.actions.cordon_node import CordonNodeAction
from kubortex.remediator.actions.drain_node import DrainNodeAction
from kubortex.remediator.actions.restart_pod import RestartPodAction
from kubortex.remediator.actions.rollback_deployment import RollbackDeploymentAction
from kubortex.remediator.actions.scale_up import ScaleUpAction

_REGISTRY: dict[str, type[BaseAction]] = {
    "restart-pod": RestartPodAction,
    "scale-up": ScaleUpAction,
    "rollback-deployment": RollbackDeploymentAction,
    "cordon-node": CordonNodeAction,
    "drain-node": DrainNodeAction,
}


def get_action(action_type: str) -> BaseAction:
    """Instantiate and return the action handler for *action_type*.

    Raises ``KeyError`` if the action type is not registered.
    """
    cls = _REGISTRY[action_type]
    return cls()


def list_action_types() -> list[str]:
    """Return all registered action type names."""
    return list(_REGISTRY.keys())
