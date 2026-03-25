"""Restart-pod action — delete the pod and let its controller recreate it."""

from __future__ import annotations

import asyncio
from typing import Any

import structlog
from kubernetes_asyncio import client as k8s_client

from .base import BaseAction

logger = structlog.get_logger(__name__)


class RestartPodAction(BaseAction):
    """Delete a pod so its owning controller (ReplicaSet/StatefulSet) recreates it."""

    async def pre_flight(self, target: dict[str, Any], parameters: dict[str, Any]) -> bool:
        core = k8s_client.CoreV1Api()
        try:
            pod = await core.read_namespaced_pod(name=target["name"], namespace=target["namespace"])
            # Ensure the pod has an owner controller
            owners = pod.metadata.owner_references or []
            if not owners:
                logger.warning("pod_has_no_owner", pod=target["name"])
                return False
            return True
        except k8s_client.ApiException as exc:
            logger.warning("pre_flight_failed", error=str(exc))
            return False

    async def dry_run(self, target: dict[str, Any], parameters: dict[str, Any]) -> dict[str, Any]:
        return {
            "action": "restart-pod",
            "target": f"{target['namespace']}/{target['name']}",
            "effect": "Pod will be deleted and recreated by its controller",
        }

    async def execute(self, target: dict[str, Any], parameters: dict[str, Any]) -> dict[str, Any]:
        core = k8s_client.CoreV1Api()
        grace = parameters.get("gracePeriodSeconds", 30)
        await core.delete_namespaced_pod(
            name=target["name"],
            namespace=target["namespace"],
            grace_period_seconds=grace,
        )
        logger.info("pod_deleted", pod=target["name"], namespace=target["namespace"])
        return {"deleted": target["name"], "gracePeriod": grace}

    async def verify(
        self, target: dict[str, Any], parameters: dict[str, Any], execution_result: dict[str, Any]
    ) -> dict[str, Any]:
        core = k8s_client.CoreV1Api()
        # Wait for replacement pod to become ready
        timeout = parameters.get("verifyTimeoutSeconds", 60)
        interval = 5
        elapsed = 0

        while elapsed < timeout:
            await asyncio.sleep(interval)
            elapsed += interval
            try:
                pods = await core.list_namespaced_pod(
                    namespace=target["namespace"],
                    label_selector=parameters.get("labelSelector", ""),
                )
                ready_pods = [
                    p
                    for p in pods.items
                    if p.status.phase == "Running"
                    and all(c.ready for c in (p.status.container_statuses or []))
                ]
                if ready_pods:
                    new_name = ready_pods[0].metadata.name
                    return {"success": True, "metric": "pod_ready", "newPod": new_name}
            except k8s_client.ApiException:
                pass

        return {"success": False, "metric": "pod_ready", "reason": "Timeout waiting for ready pod"}

    async def rollback(
        self, target: dict[str, Any], parameters: dict[str, Any], execution_result: dict[str, Any]
    ) -> dict[str, Any]:
        # Pod restart is not directly reversible — escalate
        return {"rolledBack": False, "reason": "Pod restart cannot be reversed; escalation needed"}
