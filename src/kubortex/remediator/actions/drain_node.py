"""Drain-node action — cordon the node then evict all non-DaemonSet pods."""

from __future__ import annotations

import asyncio
from typing import Any

import structlog
from kubernetes_asyncio import client as k8s_client

from .base import BaseAction

logger = structlog.get_logger(__name__)


class DrainNodeAction(BaseAction):
    """Evict all eligible pods from a node after cordoning it."""

    async def pre_flight(self, target: dict[str, Any], parameters: dict[str, Any]) -> bool:
        core = k8s_client.CoreV1Api()
        try:
            await core.read_node(name=target["name"])
            return True
        except k8s_client.ApiException as exc:
            logger.warning("pre_flight_failed", error=str(exc))
            return False

    async def dry_run(self, target: dict[str, Any], parameters: dict[str, Any]) -> dict[str, Any]:
        core = k8s_client.CoreV1Api()
        node_name = target["name"]
        pods = await core.list_pod_for_all_namespaces(field_selector=f"spec.nodeName={node_name}")
        evictable = [
            p.metadata.name
            for p in pods.items
            if not _is_daemonset_pod(p) and not _is_mirror_pod(p)
        ]
        return {
            "action": "drain-node",
            "target": target["name"],
            "podsToEvict": len(evictable),
            "podNames": evictable[:20],  # cap preview
        }

    async def execute(self, target: dict[str, Any], parameters: dict[str, Any]) -> dict[str, Any]:
        core = k8s_client.CoreV1Api()

        # Capture schedulability state before we touch anything
        node = await core.read_node(name=target["name"])
        was_already_cordoned = bool(node.spec.unschedulable)

        # Cordon first
        await core.patch_node(
            name=target["name"],
            body={"spec": {"unschedulable": True}},
        )

        # List pods to evict
        node_name = target["name"]
        pods = await core.list_pod_for_all_namespaces(field_selector=f"spec.nodeName={node_name}")
        evicted: list[str] = []
        failed: list[str] = []

        grace = parameters.get("gracePeriodSeconds", 30)

        for pod in pods.items:
            if _is_daemonset_pod(pod) or _is_mirror_pod(pod):
                continue

            pod_name = pod.metadata.name
            pod_ns = pod.metadata.namespace

            eviction = k8s_client.V1Eviction(
                metadata=k8s_client.V1ObjectMeta(name=pod_name, namespace=pod_ns),
                delete_options=k8s_client.V1DeleteOptions(grace_period_seconds=grace),
            )

            try:
                await core.create_namespaced_pod_eviction(
                    name=pod_name, namespace=pod_ns, body=eviction
                )
                evicted.append(f"{pod_ns}/{pod_name}")
            except k8s_client.ApiException as exc:
                logger.warning("eviction_failed", pod=pod_name, error=str(exc))
                failed.append(f"{pod_ns}/{pod_name}")

        logger.info("node_drained", node=target["name"], evicted=len(evicted), failed=len(failed))
        return {
            "node": target["name"],
            "evicted": evicted,
            "failed": failed,
            "wasAlreadyCordoned": was_already_cordoned,
        }

    async def verify(
        self, target: dict[str, Any], parameters: dict[str, Any], execution_result: dict[str, Any]
    ) -> dict[str, Any]:
        core = k8s_client.CoreV1Api()
        timeout = parameters.get("verifyTimeoutSeconds", 120)
        interval = 10
        elapsed = 0

        while elapsed < timeout:
            await asyncio.sleep(interval)
            elapsed += interval

            pods = await core.list_pod_for_all_namespaces(
                field_selector=f"spec.nodeName={target['name']}"
            )
            non_ds = [p for p in pods.items if not _is_daemonset_pod(p) and not _is_mirror_pod(p)]
            if not non_ds:
                return {"improved": True, "metric": "pods_remaining", "after": 0}

        remaining = len(non_ds)
        return {"improved": False, "metric": "pods_remaining", "after": remaining}

    async def rollback(
        self, target: dict[str, Any], parameters: dict[str, Any], execution_result: dict[str, Any]
    ) -> dict[str, Any]:
        core = k8s_client.CoreV1Api()
        was_already_cordoned = execution_result.get("wasAlreadyCordoned", False)
        if not was_already_cordoned:
            # Only uncordon if we cordoned it — don't alter pre-existing state
            await core.patch_node(
                name=target["name"],
                body={"spec": {"unschedulable": False}},
            )
            logger.info("node_uncordoned_after_drain_rollback", node=target["name"])
            return {
                "triggered": True,
                "uncordoned": True,
                "note": "Evicted pods cannot be restored",
            }
        else:
            logger.info("node_left_cordoned_after_drain_rollback", node=target["name"])
            return {
                "triggered": True,
                "uncordoned": False,
                "note": "Node was already cordoned before drain; left cordoned. Evicted pods cannot be restored",
            }


def _is_daemonset_pod(pod: Any) -> bool:
    return any(ref.kind == "DaemonSet" for ref in pod.metadata.owner_references or [])


def _is_mirror_pod(pod: Any) -> bool:
    annotations = pod.metadata.annotations or {}
    return "kubernetes.io/config.mirror" in annotations
