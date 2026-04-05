"""Rollback-deployment action — revert a Deployment to a previous revision."""

from __future__ import annotations

from typing import Any

import structlog
from kubernetes_asyncio import client as k8s_client

from .base import BaseAction

logger = structlog.get_logger(__name__)


class RollbackDeploymentAction(BaseAction):
    """Roll back a Deployment to a specified or previous revision."""

    async def pre_flight(self, target: dict[str, Any], parameters: dict[str, Any]) -> bool:
        apps = k8s_client.AppsV1Api()
        try:
            dep = await apps.read_namespaced_deployment(
                name=target["name"], namespace=target["namespace"]
            )
            # Verify the deployment exists and has history
            revision = (dep.metadata.annotations or {}).get(
                "deployment.kubernetes.io/revision", "0"
            )
            if int(revision) < 2:
                logger.warning("no_previous_revision", deployment=target["name"])
                return False
            return True
        except k8s_client.ApiException as exc:
            logger.warning("pre_flight_failed", error=str(exc))
            return False

    async def dry_run(self, target: dict[str, Any], parameters: dict[str, Any]) -> dict[str, Any]:
        return {
            "action": "rollback-deployment",
            "target": f"{target['namespace']}/{target['name']}",
            "toRevision": parameters.get("revision", "previous"),
        }

    async def execute(self, target: dict[str, Any], parameters: dict[str, Any]) -> dict[str, Any]:
        apps = k8s_client.AppsV1Api()

        # Read current deployment to capture full container list for potential rollback
        dep = await apps.read_namespaced_deployment(
            name=target["name"], namespace=target["namespace"]
        )
        previous_containers = [
            {"name": c.name, "image": c.image}
            for c in dep.spec.template.spec.containers
        ]

        # Get the target revision's ReplicaSet
        revision = parameters.get("revision")
        rs_list = await apps.list_namespaced_replica_set(
            namespace=target["namespace"],
            label_selector=",".join(
                f"{k}={v}" for k, v in (dep.spec.selector.match_labels or {}).items()
            ),
        )

        target_rs = None
        if revision:
            for rs in rs_list.items:
                rs_rev = (rs.metadata.annotations or {}).get(
                    "deployment.kubernetes.io/revision", ""
                )
                if rs_rev == str(revision):
                    target_rs = rs
                    break
        else:
            # Find the second-most-recent ReplicaSet by revision number
            sorted_rs = sorted(
                rs_list.items,
                key=lambda r: int(
                    (r.metadata.annotations or {}).get("deployment.kubernetes.io/revision", "0")
                ),
                reverse=True,
            )
            if len(sorted_rs) >= 2:
                target_rs = sorted_rs[1]

        if not target_rs:
            return {"improved": False, "error": "Could not find target revision ReplicaSet"}

        rolled_back_containers = [
            {"name": c.name, "image": c.image}
            for c in target_rs.spec.template.spec.containers
        ]

        # Patch the deployment with the old template (preserves all containers)
        await apps.patch_namespaced_deployment(
            name=target["name"],
            namespace=target["namespace"],
            body={"spec": {"template": target_rs.spec.template}},
        )

        logger.info(
            "deployment_rolled_back",
            deployment=target["name"],
            from_images=[c["image"] for c in previous_containers],
            to_images=[c["image"] for c in rolled_back_containers],
        )
        return {
            "previousContainers": previous_containers,
            "rolledBackContainers": rolled_back_containers,
        }

    async def verify(
        self, target: dict[str, Any], parameters: dict[str, Any], execution_result: dict[str, Any]
    ) -> dict[str, Any]:
        apps = k8s_client.AppsV1Api()
        dep = await apps.read_namespaced_deployment(
            name=target["name"], namespace=target["namespace"]
        )

        available = dep.status.available_replicas or 0
        desired = dep.spec.replicas or 1
        progressing = any(
            c.type == "Progressing" and c.status == "True" for c in (dep.status.conditions or [])
        )

        improved = available >= desired and not progressing
        return {
            "improved": improved,
            "metric": "deployment_available",
            "before": execution_result.get("previousContainers"),
            "after": execution_result.get("rolledBackContainers"),
        }

    async def rollback(
        self, target: dict[str, Any], parameters: dict[str, Any], execution_result: dict[str, Any]
    ) -> dict[str, Any]:
        # Rollback of a rollback = re-apply the original containers
        apps = k8s_client.AppsV1Api()
        previous_containers = execution_result.get("previousContainers", [])
        if not previous_containers:
            return {"triggered": False, "reason": "No original containers recorded"}

        await apps.patch_namespaced_deployment(
            name=target["name"],
            namespace=target["namespace"],
            body={
                "spec": {
                    "template": {
                        "spec": {"containers": previous_containers}
                    }
                }
            },
        )
        logger.info(
            "rollback_reversed",
            deployment=target["name"],
            images=[c["image"] for c in previous_containers],
        )
        return {"triggered": True, "restoredContainers": previous_containers}
