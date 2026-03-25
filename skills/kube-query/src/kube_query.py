"""Kube-query skill — read-only Kubernetes resource queries."""

from __future__ import annotations

import json
from typing import Any

from kubernetes_asyncio import client as k8s_client

from kubortex.investigator.skills.models import SkillInput, SkillResult

_SUPPORTED = {"pods", "events", "deployments", "nodes", "replicasets"}


class KubeQuerySkill:
    """Read-only Kubernetes inspection skill."""

    async def execute(self, inp: SkillInput) -> SkillResult:
        resource_type = inp.query.lower()
        if resource_type not in _SUPPORTED:
            return SkillResult(
                success=False,
                error=f"unsupported resource type: {resource_type}",
            )

        ns = inp.namespace or "default"
        label_selector = inp.parameters.get("label_selector", "")
        name = inp.parameters.get("name", "")

        try:
            data = await _query(resource_type, ns, name, str(label_selector))
            raw = json.dumps(data, default=str)
            summary = _summarise(resource_type, data)
            return SkillResult(
                success=True,
                data=data,
                summary=summary,
                raw_size=len(raw),
            )
        except Exception as exc:
            return SkillResult(success=False, error=str(exc))


async def _query(
    resource_type: str,
    namespace: str,
    name: str,
    label_selector: str,
) -> Any:
    """Execute the appropriate Kubernetes API call."""
    core = k8s_client.CoreV1Api()
    apps = k8s_client.AppsV1Api()

    if resource_type == "pods":
        if name:
            pod = await core.read_namespaced_pod(name, namespace)
            return _pod_summary(pod)
        result = await core.list_namespaced_pod(namespace, label_selector=label_selector)
        return [_pod_summary(p) for p in result.items]

    if resource_type == "events":
        result = await core.list_namespaced_event(namespace)
        return [_event_summary(e) for e in result.items[-50:]]

    if resource_type == "deployments":
        if name:
            dep = await apps.read_namespaced_deployment(name, namespace)
            return _deployment_summary(dep)
        result = await apps.list_namespaced_deployment(namespace, label_selector=label_selector)
        return [_deployment_summary(d) for d in result.items]

    if resource_type == "nodes":
        result = await core.list_node()
        return [_node_summary(n) for n in result.items]

    if resource_type == "replicasets":
        result = await apps.list_namespaced_replica_set(namespace, label_selector=label_selector)
        return [_rs_summary(rs) for rs in result.items]

    return []


def _pod_summary(pod: Any) -> dict[str, Any]:
    return {
        "name": pod.metadata.name,
        "namespace": pod.metadata.namespace,
        "phase": pod.status.phase,
        "restarts": sum((cs.restart_count or 0) for cs in (pod.status.container_statuses or [])),
        "node": pod.spec.node_name,
    }


def _event_summary(event: Any) -> dict[str, Any]:
    return {
        "type": event.type,
        "reason": event.reason,
        "message": event.message,
        "count": event.count,
        "last_timestamp": str(event.last_timestamp),
        "involved_object": f"{event.involved_object.kind}/{event.involved_object.name}",
    }


def _deployment_summary(dep: Any) -> dict[str, Any]:
    return {
        "name": dep.metadata.name,
        "replicas": dep.spec.replicas,
        "ready": dep.status.ready_replicas or 0,
        "updated": dep.status.updated_replicas or 0,
        "available": dep.status.available_replicas or 0,
    }


def _node_summary(node: Any) -> dict[str, Any]:
    conditions = {c.type: c.status for c in (node.status.conditions or [])}
    return {
        "name": node.metadata.name,
        "conditions": conditions,
        "unschedulable": node.spec.unschedulable or False,
    }


def _rs_summary(rs: Any) -> dict[str, Any]:
    return {
        "name": rs.metadata.name,
        "replicas": rs.spec.replicas,
        "ready": rs.status.ready_replicas or 0,
    }


def _summarise(resource_type: str, data: Any) -> str:
    if isinstance(data, list):
        return f"Found {len(data)} {resource_type}"
    return f"Retrieved {resource_type} details"


def create() -> KubeQuerySkill:
    """Factory function referenced by the skill entrypoint."""
    return KubeQuerySkill()
