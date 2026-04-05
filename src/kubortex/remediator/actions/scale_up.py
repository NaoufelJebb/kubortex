"""Scale-up action — increase replica count of a Deployment or StatefulSet."""

from __future__ import annotations

import asyncio
from typing import Any

import structlog
from kubernetes_asyncio import client as k8s_client

from .base import BaseAction

logger = structlog.get_logger(__name__)


class ScaleUpAction(BaseAction):
    """Increase the replica count of a workload controller."""

    async def pre_flight(self, target: dict[str, Any], parameters: dict[str, Any]) -> bool:
        apps = k8s_client.AppsV1Api()
        kind = target.get("kind", "Deployment")
        try:
            if kind == "Deployment":
                dep = await apps.read_namespaced_deployment(
                    name=target["name"], namespace=target["namespace"]
                )
                current = dep.spec.replicas or 1
            elif kind == "StatefulSet":
                sts = await apps.read_namespaced_stateful_set(
                    name=target["name"], namespace=target["namespace"]
                )
                current = sts.spec.replicas or 1
            else:
                logger.warning("unsupported_kind_for_scale", kind=kind)
                return False

            desired = parameters.get("replicas", current + 1)
            if desired <= current:
                logger.warning("scale_not_needed", current=current, desired=desired)
                return False
            return True
        except k8s_client.ApiException as exc:
            logger.warning("pre_flight_failed", error=str(exc))
            return False

    async def dry_run(self, target: dict[str, Any], parameters: dict[str, Any]) -> dict[str, Any]:
        return {
            "action": "scale-up",
            "target": f"{target['namespace']}/{target['name']}",
            "replicas": parameters.get("replicas", "current + 1"),
        }

    async def execute(self, target: dict[str, Any], parameters: dict[str, Any]) -> dict[str, Any]:
        apps = k8s_client.AppsV1Api()
        kind = target.get("kind", "Deployment")

        if kind == "Deployment":
            dep = await apps.read_namespaced_deployment(
                name=target["name"], namespace=target["namespace"]
            )
            original = dep.spec.replicas or 1
            desired = parameters.get("replicas", original + 1)
            await apps.patch_namespaced_deployment_scale(
                name=target["name"],
                namespace=target["namespace"],
                body={"spec": {"replicas": desired}},
            )
        else:
            sts = await apps.read_namespaced_stateful_set(
                name=target["name"], namespace=target["namespace"]
            )
            original = sts.spec.replicas or 1
            desired = parameters.get("replicas", original + 1)
            await apps.patch_namespaced_stateful_set_scale(
                name=target["name"],
                namespace=target["namespace"],
                body={"spec": {"replicas": desired}},
            )

        logger.info("scaled_up", target=target["name"], original=original, desired=desired)
        return {"originalReplicas": original, "newReplicas": desired}

    async def verify(
        self, target: dict[str, Any], parameters: dict[str, Any], execution_result: dict[str, Any]
    ) -> dict[str, Any]:
        apps = k8s_client.AppsV1Api()
        timeout = parameters.get("verifyTimeoutSeconds", 120)
        interval = 10
        elapsed = 0
        desired = execution_result["newReplicas"]

        while elapsed < timeout:
            await asyncio.sleep(interval)
            elapsed += interval
            try:
                if target.get("kind", "Deployment") == "Deployment":
                    dep = await apps.read_namespaced_deployment(
                        name=target["name"], namespace=target["namespace"]
                    )
                    ready = dep.status.ready_replicas or 0
                else:
                    sts = await apps.read_namespaced_stateful_set(
                        name=target["name"], namespace=target["namespace"]
                    )
                    ready = sts.status.ready_replicas or 0

                if ready >= desired:
                    return {
                        "improved": True,
                        "metric": "ready_replicas",
                        "before": execution_result["originalReplicas"],
                        "after": ready,
                    }
            except k8s_client.ApiException:
                pass

        return {
            "improved": False,
            "metric": "ready_replicas",
            "reason": "Timeout waiting for replicas",
        }

    async def rollback(
        self, target: dict[str, Any], parameters: dict[str, Any], execution_result: dict[str, Any]
    ) -> dict[str, Any]:
        apps = k8s_client.AppsV1Api()
        original = execution_result["originalReplicas"]

        if target.get("kind", "Deployment") == "Deployment":
            await apps.patch_namespaced_deployment_scale(
                name=target["name"],
                namespace=target["namespace"],
                body={"spec": {"replicas": original}},
            )
        else:
            await apps.patch_namespaced_stateful_set_scale(
                name=target["name"],
                namespace=target["namespace"],
                body={"spec": {"replicas": original}},
            )

        logger.info("scale_rolled_back", target=target["name"], replicas=original)
        return {"triggered": True, "restoredReplicas": original}
