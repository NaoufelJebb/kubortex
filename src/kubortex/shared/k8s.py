"""Thin async helpers around kubernetes_asyncio for CRD operations.

All helpers operate on the ``kubortex.io`` API group and use the status
sub-resource where appropriate.  The claim protocol implements optimistic
concurrency via ``resourceVersion`` compare-and-swap on ``claimedBy``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import structlog
from kubernetes_asyncio import client as k8s_client
from kubernetes_asyncio.client import ApiException

from kubortex.shared.config import KubortexSettings

logger = structlog.get_logger(__name__)

# AIDEV-NOTE: All CRD operations go through CustomObjectsApi to stay
# generic across Incident, Investigation, ActionExecution, etc.

_SETTINGS: KubortexSettings | None = None


def _settings() -> KubortexSettings:
    global _SETTINGS
    if _SETTINGS is None:
        _SETTINGS = KubortexSettings()
    return _SETTINGS


def _api() -> k8s_client.CustomObjectsApi:
    return k8s_client.CustomObjectsApi()


# ---------------------------------------------------------------------------
# Generic CRUD
# ---------------------------------------------------------------------------


async def get_resource(plural: str, name: str, *, namespace: str | None = None) -> dict[str, Any]:
    """GET a single custom resource by name."""
    s = _settings()
    ns = namespace or s.namespace
    return await _api().get_namespaced_custom_object(
        group=s.crd_group, version=s.crd_version, namespace=ns, plural=plural, name=name
    )


async def list_resources(plural: str, *, namespace: str | None = None) -> list[dict[str, Any]]:
    """LIST custom resources of a given kind."""
    s = _settings()
    ns = namespace or s.namespace
    result = await _api().list_namespaced_custom_object(
        group=s.crd_group, version=s.crd_version, namespace=ns, plural=plural
    )
    return result.get("items", [])


async def create_resource(
    plural: str, body: dict[str, Any], *, namespace: str | None = None
) -> dict[str, Any]:
    """CREATE a new custom resource."""
    s = _settings()
    ns = namespace or s.namespace
    return await _api().create_namespaced_custom_object(
        group=s.crd_group, version=s.crd_version, namespace=ns, plural=plural, body=body
    )


async def patch_status(
    plural: str, name: str, status_patch: dict[str, Any], *, namespace: str | None = None
) -> dict[str, Any]:
    """PATCH the status sub-resource of a custom resource."""
    s = _settings()
    ns = namespace or s.namespace
    return await _api().patch_namespaced_custom_object_status(
        group=s.crd_group,
        version=s.crd_version,
        namespace=ns,
        plural=plural,
        name=name,
        body={"status": status_patch},
    )


# ---------------------------------------------------------------------------
# Claim protocol (optimistic concurrency)
# ---------------------------------------------------------------------------


async def try_claim(
    plural: str, name: str, pod_name: str, *, namespace: str | None = None
) -> bool:
    """Attempt to claim a resource via compare-and-swap on ``claimedBy``.

    Returns *True* if this pod successfully claimed the resource, *False* if
    another worker won the race (HTTP 409 Conflict).
    """
    s = _settings()
    ns = namespace or s.namespace
    resource = await get_resource(plural, name, namespace=ns)
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
        await _api().patch_namespaced_custom_object_status(
            group=s.crd_group,
            version=s.crd_version,
            namespace=ns,
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
