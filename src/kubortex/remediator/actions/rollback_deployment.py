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
            revision = dep.metadata.annotations.get("deployment.kubernetes.io/revision", "0")
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

        # Read current deployment to capture state
        dep = await apps.read_namespaced_deployment(
            name=target["name"], namespace=target["namespace"]
        )
        current_image = ""
        if dep.spec.template.spec.containers:
            current_image = dep.spec.template.spec.containers[0].image

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
            return {"success": False, "error": "Could not find target revision ReplicaSet"}

        # Patch the deployment with the old template
        rollback_image = ""
        if target_rs.spec.template.spec.containers:
            rollback_image = target_rs.spec.template.spec.containers[0].image

        await apps.patch_namespaced_deployment(
            name=target["name"],
            namespace=target["namespace"],
            body={"spec": {"template": target_rs.spec.template}},
        )

        logger.info(
            "deployment_rolled_back",
            deployment=target["name"],
            from_image=current_image,
            to_image=rollback_image,
        )
        return {
            "previousImage": current_image,
            "rolledBackImage": rollback_image,
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

        success = available >= desired and not progressing
        return {
            "success": success,
            "metric": "deployment_available",
            "before": execution_result.get("previousImage"),
            "after": execution_result.get("rolledBackImage"),
        }

    async def rollback(
        self, target: dict[str, Any], parameters: dict[str, Any], execution_result: dict[str, Any]
    ) -> dict[str, Any]:
        # Rollback of a rollback = re-apply the original image
        apps = k8s_client.AppsV1Api()
        original_image = execution_result.get("previousImage")
        if not original_image:
            return {"rolledBack": False, "reason": "No original image recorded"}

        await apps.patch_namespaced_deployment(
            name=target["name"],
            namespace=target["namespace"],
            body={
                "spec": {
                    "template": {
                        "spec": {"containers": [{"name": target["name"], "image": original_image}]}
                    }
                }
            },
        )
        logger.info("rollback_reversed", deployment=target["name"], image=original_image)
        return {"rolledBack": True, "restoredImage": original_image}
