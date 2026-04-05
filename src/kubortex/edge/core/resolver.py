"""Source-agnostic target resolution for edge signals."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

import structlog
from kubernetes_asyncio import client as k8s_client
from kubernetes_asyncio.client import ApiException

from kubortex.shared.models.incident import TargetRef

logger = structlog.get_logger(__name__)

# Deployment pods:   <name>-<replicaset-hash>-<pod-hash>  (8-10 + 5 lowercase alphanum chars)
# StatefulSet pods:  <name>-<ordinal>
_RS_SUFFIX = re.compile(r"-[a-z0-9]{8,10}-[a-z0-9]{5}$")
_STS_SUFFIX = re.compile(r"-\d+$")


@dataclass(slots=True)
class TargetHints:
    """Source-agnostic target identity hints extracted from a signal payload."""

    namespace: str = ""
    pod: str = ""
    deployment: str = ""
    statefulset: str = ""
    daemonset: str = ""
    service: str = ""
    ingress: str = ""
    node: str = ""
    pvc: str = ""
    raw_labels: dict[str, str] = field(default_factory=dict)


def _infer_workload_from_pod_name(pod_name: str) -> tuple[str, str] | None:
    """Infer a workload owner from a Kubernetes pod name.

    Args:
        pod_name: Pod name to inspect.

    Returns:
        ``(kind, name)`` for a recognized workload owner, or ``None``.
    """
    m = _RS_SUFFIX.search(pod_name)
    if m:
        base = pod_name[: m.start()]
        if base:
            return ("Deployment", base)

    m = _STS_SUFFIX.search(pod_name)
    if m:
        base = pod_name[: m.start()]
        if base:
            return ("StatefulSet", base)

    return None


async def _read_pod_resource(namespace: str, name: str) -> dict[str, Any] | None:
    """Fetch a Pod for enrichment.

    Args:
        namespace: Pod namespace.
        name: Pod name.

    Returns:
        Pod resource object, or ``None`` on 404 or read failure.
    """
    try:
        async with k8s_client.ApiClient() as api_client:
            api = k8s_client.CoreV1Api(api_client)
            return await api.read_namespaced_pod(name=name, namespace=namespace)
    except ApiException as exc:
        if exc.status != 404:
            logger.warning("target_resolver_pod_lookup_failed", namespace=namespace, pod=name)
        return None
    except Exception:
        logger.warning("target_resolver_pod_lookup_failed", namespace=namespace, pod=name)
        return None


async def _read_replicaset_resource(namespace: str, name: str) -> dict[str, Any] | None:
    """Fetch a ReplicaSet for enrichment.

    Args:
        namespace: ReplicaSet namespace.
        name: ReplicaSet name.

    Returns:
        ReplicaSet resource object, or ``None`` on 404 or read failure.
    """
    try:
        async with k8s_client.ApiClient() as api_client:
            api = k8s_client.AppsV1Api(api_client)
            return await api.read_namespaced_replica_set(name=name, namespace=namespace)
    except ApiException as exc:
        if exc.status != 404:
            logger.warning(
                "target_resolver_replicaset_lookup_failed",
                namespace=namespace,
                replicaset=name,
            )
        return None
    except Exception:
        logger.warning(
            "target_resolver_replicaset_lookup_failed",
            namespace=namespace,
            replicaset=name,
        )
        return None


def _owner_target_from_refs(namespace: str, refs: list[dict[str, Any]]) -> TargetRef | None:
    """Resolve a supported owner target from owner references.

    Args:
        namespace: Namespace for the resolved target.
        refs: Owner references represented as ``{"kind": ..., "name": ...}``.

    Returns:
        Resolved owner target when supported, else ``None``.
    """
    for ref in refs:
        kind = ref.get("kind", "")
        name = ref.get("name", "")
        if kind in {"StatefulSet", "DaemonSet"} and name:
            return TargetRef(kind=kind, namespace=namespace, name=name)
    return None


async def _resolve_pod_owner(hints: TargetHints) -> TargetRef | None:
    """Resolve a workload owner from a pod hint using bounded API enrichment.

    Args:
        hints: Target identity hints.

    Returns:
        Resolved workload or pod target, or ``None`` when enrichment fails.
    """
    if not hints.pod or not hints.namespace:
        return None

    pod = await _read_pod_resource(hints.namespace, hints.pod)
    if pod is None:
        return None

    metadata = getattr(pod, "metadata", None)
    refs = list(getattr(metadata, "owner_references", None) or [])
    raw_refs = [{"kind": ref.kind, "name": ref.name} for ref in refs if ref.kind and ref.name]

    target = _owner_target_from_refs(hints.namespace, raw_refs)
    if target is not None:
        return target

    for ref in raw_refs:
        if ref["kind"] != "ReplicaSet":
            continue
        rs = await _read_replicaset_resource(hints.namespace, ref["name"])
        if rs is None:
            continue
        rs_meta = getattr(rs, "metadata", None)
        rs_refs = list(getattr(rs_meta, "owner_references", None) or [])
        for rs_ref in rs_refs:
            if rs_ref.kind == "Deployment" and rs_ref.name:
                return TargetRef(kind="Deployment", namespace=hints.namespace, name=rs_ref.name)

    return TargetRef(kind="Pod", namespace=hints.namespace, name=hints.pod)


async def resolve_target(hints: TargetHints) -> TargetRef | None:
    """Resolve the best operational target from source-agnostic hints.

    Resolution prefers explicit resource identity first, then workload
    inference, then bounded Kubernetes API enrichment, then broader namespace
    targeting.

    Args:
        hints: Source-agnostic target identity hints.

    Returns:
        Best-effort target reference, or ``None`` when no trustworthy target
        can be inferred.
    """
    if hints.deployment and hints.namespace:
        return TargetRef(kind="Deployment", namespace=hints.namespace, name=hints.deployment)
    if hints.statefulset and hints.namespace:
        return TargetRef(kind="StatefulSet", namespace=hints.namespace, name=hints.statefulset)
    if hints.daemonset and hints.namespace:
        return TargetRef(kind="DaemonSet", namespace=hints.namespace, name=hints.daemonset)
    if hints.service and hints.namespace:
        return TargetRef(kind="Service", namespace=hints.namespace, name=hints.service)
    if hints.ingress and hints.namespace:
        return TargetRef(kind="Ingress", namespace=hints.namespace, name=hints.ingress)
    if hints.pvc and hints.namespace:
        return TargetRef(kind="PersistentVolumeClaim", namespace=hints.namespace, name=hints.pvc)
    if hints.node:
        return TargetRef(kind="Node", namespace="", name=hints.node)

    if hints.pod and hints.namespace:
        inferred = _infer_workload_from_pod_name(hints.pod)
        if inferred:
            return TargetRef(kind=inferred[0], namespace=hints.namespace, name=inferred[1])

    enriched = await _resolve_pod_owner(hints)
    if enriched is not None:
        return enriched

    if hints.namespace:
        return TargetRef(kind="Namespace", namespace="", name=hints.namespace)

    return None
