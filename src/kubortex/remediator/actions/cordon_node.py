"""Cordon-node action — mark a node as unschedulable."""

from __future__ import annotations

from typing import Any

import structlog
from kubernetes_asyncio import client as k8s_client

from .base import BaseAction

logger = structlog.get_logger(__name__)


class CordonNodeAction(BaseAction):
    """Set a node's ``spec.unschedulable`` to True."""

    async def pre_flight(self, target: dict[str, Any], parameters: dict[str, Any]) -> bool:
        core = k8s_client.CoreV1Api()
        try:
            node = await core.read_node(name=target["name"])
            if node.spec.unschedulable:
                logger.warning("node_already_cordoned", node=target["name"])
                return False
            return True
        except k8s_client.ApiException as exc:
            logger.warning("pre_flight_failed", error=str(exc))
            return False

    async def dry_run(self, target: dict[str, Any], parameters: dict[str, Any]) -> dict[str, Any]:
        return {
            "action": "cordon-node",
            "target": target["name"],
            "effect": "Node will be marked unschedulable",
        }

    async def execute(self, target: dict[str, Any], parameters: dict[str, Any]) -> dict[str, Any]:
        core = k8s_client.CoreV1Api()
        await core.patch_node(
            name=target["name"],
            body={"spec": {"unschedulable": True}},
        )
        logger.info("node_cordoned", node=target["name"])
        return {"cordoned": True, "node": target["name"]}

    async def verify(
        self, target: dict[str, Any], parameters: dict[str, Any], execution_result: dict[str, Any]
    ) -> dict[str, Any]:
        core = k8s_client.CoreV1Api()
        node = await core.read_node(name=target["name"])
        return {
            "improved": bool(node.spec.unschedulable),
            "metric": "node_unschedulable",
            "after": node.spec.unschedulable,
        }

    async def rollback(
        self, target: dict[str, Any], parameters: dict[str, Any], execution_result: dict[str, Any]
    ) -> dict[str, Any]:
        core = k8s_client.CoreV1Api()
        await core.patch_node(
            name=target["name"],
            body={"spec": {"unschedulable": False}},
        )
        logger.info("node_uncordoned", node=target["name"])
        return {"triggered": True, "node": target["name"]}
