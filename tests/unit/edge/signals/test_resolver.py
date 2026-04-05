"""Unit tests for kubortex.edge.core.resolver."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from kubortex.edge.core.resolver import TargetHints, resolve_target


class TestResolveTarget:
    @pytest.mark.asyncio
    async def test_explicit_deployment_wins(self) -> None:
        target = await resolve_target(
            TargetHints(namespace="prod", deployment="api", pod="api-123", node="worker-1")
        )
        assert target is not None
        assert target.kind == "Deployment"
        assert target.namespace == "prod"
        assert target.name == "api"

    @pytest.mark.asyncio
    async def test_explicit_service_resolves(self) -> None:
        target = await resolve_target(TargetHints(namespace="prod", service="payments"))
        assert target is not None
        assert target.kind == "Service"
        assert target.namespace == "prod"
        assert target.name == "payments"

    @pytest.mark.asyncio
    async def test_explicit_ingress_resolves(self) -> None:
        target = await resolve_target(TargetHints(namespace="prod", ingress="payments-ing"))
        assert target is not None
        assert target.kind == "Ingress"

    @pytest.mark.asyncio
    async def test_explicit_pvc_resolves(self) -> None:
        target = await resolve_target(TargetHints(namespace="prod", pvc="data-api-0"))
        assert target is not None
        assert target.kind == "PersistentVolumeClaim"

    @pytest.mark.asyncio
    async def test_explicit_node_resolves(self) -> None:
        target = await resolve_target(TargetHints(node="worker-1"))
        assert target is not None
        assert target.kind == "Node"
        assert target.namespace == ""
        assert target.name == "worker-1"

    @pytest.mark.asyncio
    async def test_pod_name_rolls_up_to_deployment(self) -> None:
        target = await resolve_target(
            TargetHints(namespace="prod", pod="payments-api-7f8b9cd4f-xkz2p")
        )
        assert target is not None
        assert target.kind == "Deployment"
        assert target.name == "payments-api"

    @pytest.mark.asyncio
    async def test_pod_name_rolls_up_to_statefulset(self) -> None:
        target = await resolve_target(TargetHints(namespace="prod", pod="redis-0"))
        assert target is not None
        assert target.kind == "StatefulSet"
        assert target.name == "redis"

    @pytest.mark.asyncio
    async def test_api_enrichment_resolves_pod_owner(self, monkeypatch: pytest.MonkeyPatch) -> None:
        async def _read_pod(namespace: str, name: str):
            return SimpleNamespace(
                metadata=SimpleNamespace(
                    owner_references=[SimpleNamespace(kind="ReplicaSet", name="api-rs")]
                )
            )

        async def _read_rs(namespace: str, name: str):
            return SimpleNamespace(
                metadata=SimpleNamespace(
                    owner_references=[SimpleNamespace(kind="Deployment", name="api")]
                )
            )

        monkeypatch.setattr("kubortex.edge.core.resolver._read_pod_resource", _read_pod)
        monkeypatch.setattr("kubortex.edge.core.resolver._read_replicaset_resource", _read_rs)

        target = await resolve_target(TargetHints(namespace="prod", pod="custom-job-pod"))

        assert target is not None
        assert target.kind == "Deployment"
        assert target.name == "api"

    @pytest.mark.asyncio
    async def test_api_enrichment_falls_back_to_pod(self, monkeypatch: pytest.MonkeyPatch) -> None:
        async def _read_pod(namespace: str, name: str):
            return SimpleNamespace(metadata=SimpleNamespace(owner_references=[]))

        async def _read_rs(namespace: str, name: str):
            return None

        monkeypatch.setattr("kubortex.edge.core.resolver._read_pod_resource", _read_pod)
        monkeypatch.setattr("kubortex.edge.core.resolver._read_replicaset_resource", _read_rs)

        target = await resolve_target(TargetHints(namespace="prod", pod="custom-job-pod"))

        assert target is not None
        assert target.kind == "Pod"
        assert target.name == "custom-job-pod"

    @pytest.mark.asyncio
    async def test_namespace_fallback_is_used_when_no_better_target_exists(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        async def _read_pod(namespace: str, name: str):
            return None

        monkeypatch.setattr("kubortex.edge.core.resolver._read_pod_resource", _read_pod)

        target = await resolve_target(TargetHints(namespace="prod"))

        assert target is not None
        assert target.kind == "Namespace"
        assert target.namespace == ""
        assert target.name == "prod"

    @pytest.mark.asyncio
    async def test_incomplete_namespaced_resource_returns_none(self) -> None:
        assert await resolve_target(TargetHints(service="payments")) is None
