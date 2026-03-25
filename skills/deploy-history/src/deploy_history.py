"""Deploy-history skill — inspect deployment rollout history via K8s API."""

from __future__ import annotations

import json
from typing import Any

from kubernetes_asyncio import client as k8s_client

from kubortex.investigator.skills.models import SkillInput, SkillResult


class DeployHistorySkill:
    """Read deployment rollout history and revision details."""

    async def execute(self, inp: SkillInput) -> SkillResult:
        deploy_name = inp.query
        namespace = inp.namespace or "default"

        if not deploy_name:
            return SkillResult(success=False, error="deployment name required")

        try:
            apps = k8s_client.AppsV1Api()

            # Get the deployment
            dep = await apps.read_namespaced_deployment(deploy_name, namespace)

            # Get associated replicasets (represent revisions)
            selector = ",".join(
                f"{k}={v}" for k, v in (dep.spec.selector.match_labels or {}).items()
            )
            rs_list = await apps.list_namespaced_replica_set(namespace, label_selector=selector)

            revisions = _extract_revisions(rs_list.items)
            current_image = _current_image(dep)

            data = {
                "deployment": deploy_name,
                "namespace": namespace,
                "currentImage": current_image,
                "currentReplicas": dep.spec.replicas,
                "revisions": revisions,
            }
            raw = json.dumps(data, default=str)
            summary = (
                f"Deployment '{deploy_name}' has {len(revisions)} revision(s). "
                f"Current image: {current_image}"
            )

            return SkillResult(
                success=True,
                data=data,
                summary=summary,
                raw_size=len(raw),
            )
        except Exception as exc:
            return SkillResult(success=False, error=f"deploy history failed: {exc}")


def _extract_revisions(replicasets: list[Any]) -> list[dict[str, Any]]:
    """Extract revision history from replicasets."""
    revisions: list[dict[str, Any]] = []
    for rs in replicasets:
        annotations = rs.metadata.annotations or {}
        revision = annotations.get("deployment.kubernetes.io/revision", "")
        images = [c.image for c in (rs.spec.template.spec.containers or [])]
        revisions.append(
            {
                "revision": revision,
                "name": rs.metadata.name,
                "replicas": rs.spec.replicas or 0,
                "images": images,
                "createdAt": str(rs.metadata.creation_timestamp),
            }
        )
    revisions.sort(key=lambda r: r.get("revision", ""), reverse=True)
    return revisions


def _current_image(dep: Any) -> str:
    """Get the primary container image of a deployment."""
    containers = dep.spec.template.spec.containers or []
    return containers[0].image if containers else ""


def create() -> DeployHistorySkill:
    """Factory function referenced by the skill entrypoint."""
    return DeployHistorySkill()
