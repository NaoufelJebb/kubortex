"""Kubortex CRD async helpers — always operates in KUBORTEX_NAMESPACE.

All operations target the namespace configured in SharedSettings.
There is intentionally no namespace parameter: all Kubortex CRDs live
in a single fixed namespace and callers should not route by namespace.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import structlog
from kubernetes_asyncio import client as k8s_client
from kubernetes_asyncio.client import ApiException

from kubortex.shared.config import SharedSettings
from kubortex.shared.kube_clients import get_kubernetes_clients

logger = structlog.get_logger(__name__)

# AIDEV-NOTE: All CRD operations go through CustomObjectsApi to stay
# generic across Incident, Investigation, ActionExecution, etc.

_SETTINGS: SharedSettings | None = None


def _settings() -> SharedSettings:
    global _SETTINGS
    if _SETTINGS is None:
        _SETTINGS = SharedSettings()
    return _SETTINGS


async def _api() -> k8s_client.CustomObjectsApi:
    clients = await get_kubernetes_clients()
    return clients.custom_objects


# ---------------------------------------------------------------------------
# Generic CRUD
# ---------------------------------------------------------------------------


async def get_resource(plural: str, name: str) -> dict[str, Any]:
    """Fetch one Kubortex custom resource from the shared namespace.

    Uses the configured CRD group/version from ``SharedSettings`` and resolves
    the resource by ``plural`` and ``name`` within the single Kubortex control
    namespace.

    Args:
        plural: CRD plural name, for example ``incidents``.
        name: Resource name.

    Returns:
        Full Kubernetes custom resource object.
    """
    s = _settings()
    api = await _api()
    return await api.get_namespaced_custom_object(
        group=s.crd_group, version=s.crd_version, namespace=s.namespace,
        plural=plural, name=name,
    )


async def list_resources(
    plural: str,
    *,
    label_selector: str | None = None,
) -> list[dict[str, Any]]:
    """List Kubortex custom resources of one kind from the shared namespace.

    Uses the configured CRD group/version from ``SharedSettings`` and returns
    the ``items`` array from the Kubernetes list response.

    Args:
        plural: CRD plural name, for example ``investigations``.
        label_selector: Optional Kubernetes label-selector string (e.g.
            ``"app.kubernetes.io/name=payments-api,environment=prod"``).
            When provided the API server filters the results server-side,
            avoiding the need to fetch and filter all resources in-process.

    Returns:
        List of Kubernetes custom resource objects.
    """
    s = _settings()
    kwargs: dict[str, Any] = {
        "group": s.crd_group,
        "version": s.crd_version,
        "namespace": s.namespace,
        "plural": plural,
    }
    if label_selector:
        kwargs["label_selector"] = label_selector
    api = await _api()
    result = await api.list_namespaced_custom_object(**kwargs)
    return result.get("items", [])


async def create_resource(plural: str, body: dict[str, Any]) -> dict[str, Any]:
    """Create a Kubortex custom resource in the shared namespace.

    The caller provides the full Kubernetes resource body, including metadata,
    spec, and any labels or annotations required by the target CRD.

    Args:
        plural: CRD plural name, for example ``actionexecutions``.
        body: Full Kubernetes custom resource body to create.

    Returns:
        Kubernetes API response for the created resource.
    """
    s = _settings()
    api = await _api()
    return await api.create_namespaced_custom_object(
        group=s.crd_group, version=s.crd_version, namespace=s.namespace,
        plural=plural, body=body,
    )


async def patch_status(
    plural: str,
    name: str,
    status_patch: dict[str, Any],
    *,
    resource_version: str | None = None,
) -> dict[str, Any]:
    """Patch the ``status`` sub-resource of a Kubortex custom resource.

    Applies a merge patch to the status sub-resource only. When
    ``resource_version`` is provided, the patch uses optimistic locking and
    fails with ``ApiException(status=409)`` if another writer updated the
    resource after that version was read.

    Args:
        plural: CRD plural name, for example ``incidents``.
        name: Resource name.
        status_patch: Status fields to merge into the resource status.
        resource_version: Optional version observed on a prior read.

    Returns:
        Kubernetes API response for the patched resource.
    """
    s = _settings()
    body: dict[str, Any] = {"status": status_patch}
    if resource_version:
        body["metadata"] = {"resourceVersion": resource_version}
    api = await _api()
    return await api.patch_namespaced_custom_object_status(
        group=s.crd_group,
        version=s.crd_version,
        namespace=s.namespace,
        plural=plural,
        name=name,
        body=body,
        _content_type="application/merge-patch+json",
    )


async def patch_spec(
    plural: str,
    name: str,
    spec_patch: dict[str, Any],
    *,
    resource_version: str | None = None,
) -> dict[str, Any]:
    """Patch ``spec`` fields of a Kubortex custom resource.

    Applies a merge patch to the resource spec only. When ``resource_version``
    is provided, the patch uses optimistic locking and fails with
    ``ApiException(status=409)`` if another writer updated the resource after
    that version was read.

    Args:
        plural: CRD plural name, for example ``remediationplans``.
        name: Resource name.
        spec_patch: Spec fields to merge into the resource spec.
        resource_version: Optional version observed on a prior read.

    Returns:
        Kubernetes API response for the patched resource.
    """
    s = _settings()
    body: dict[str, Any] = {"spec": spec_patch}
    if resource_version:
        body["metadata"] = {"resourceVersion": resource_version}
    api = await _api()
    return await api.patch_namespaced_custom_object(
        group=s.crd_group,
        version=s.crd_version,
        namespace=s.namespace,
        plural=plural,
        name=name,
        body=body,
        _content_type="application/merge-patch+json",
    )


def resource_created_at(resource: dict[str, Any]) -> datetime:
    """Read a resource creation timestamp from Kubernetes metadata.

    Uses ``metadata.creationTimestamp`` when present. Missing timestamps are
    treated as the earliest UTC instant so callers can safely compare the
    result against real creation times without additional ``None`` handling.

    Args:
        resource: Kubernetes resource object.

    Returns:
        Parsed creation timestamp, or a minimal UTC datetime when absent.
    """
    created = resource.get("metadata", {}).get("creationTimestamp", "")
    if created:
        return datetime.fromisoformat(created.replace("Z", "+00:00"))
    return datetime.min.replace(tzinfo=UTC)


# ---------------------------------------------------------------------------
# Claim protocol (optimistic concurrency)
# ---------------------------------------------------------------------------


async def try_claim(plural: str, name: str, pod_name: str) -> bool:
    """Attempt to claim a resource via optimistic compare-and-swap on status.

    Reads the current resource, checks whether ``status.claimedBy`` is already
    set, and if not patches ``claimedBy`` and ``claimedAt`` with the observed
    ``resourceVersion``. This makes claims safe under concurrent workers.

    Returns ``True`` when this worker wins the claim and ``False`` when the
    resource was already claimed or another writer wins the 409 race.

    Args:
        plural: CRD plural name for the claimable resource kind.
        name: Resource name.
        pod_name: Worker identity to store in ``status.claimedBy``.

    Returns:
        ``True`` if the claim succeeded, else ``False``.
    """
    s = _settings()
    resource = await get_resource(plural, name)
    rv = resource["metadata"]["resourceVersion"]
    current_claim = (resource.get("status") or {}).get("claimedBy", "")

    if current_claim:
        logger.debug("resource_already_claimed", resource=name, claimed_by=current_claim)
        return False

    patch_body: dict[str, Any] = {
        "metadata": {"resourceVersion": rv},
        "status": {
            "claimedBy": pod_name,
            "claimedAt": datetime.now(UTC).isoformat(),
        },
    }

    try:
        api = await _api()
        await api.patch_namespaced_custom_object_status(
            group=s.crd_group,
            version=s.crd_version,
            namespace=s.namespace,
            plural=plural,
            name=name,
            body=patch_body,
        )
        logger.info("resource_claimed", resource=name, pod=pod_name)
        return True
    except ApiException as exc:
        if exc.status == 409:
            logger.debug("claim_conflict", resource=name, pod=pod_name)
            return False
        raise
