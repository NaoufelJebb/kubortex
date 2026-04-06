"""Unit tests for kubortex.shared.crds."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from kubortex.shared import crds


class TestResourceCreatedAt:
    def test_returns_creation_timestamp_when_present(self) -> None:
        resource = {"metadata": {"creationTimestamp": "2026-04-04T12:00:00Z"}}

        created_at = crds.resource_created_at(resource)

        assert created_at == datetime(2026, 4, 4, 12, 0, tzinfo=UTC)

    def test_returns_min_datetime_when_timestamp_absent(self) -> None:
        created_at = crds.resource_created_at({"metadata": {}})

        assert created_at == datetime.min.replace(tzinfo=UTC)


class TestPatchSpec:
    @pytest.mark.asyncio
    async def test_uses_plain_spec_patch_without_resource_version(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        api = SimpleNamespace(
            patch_namespaced_custom_object=AsyncMock(return_value={"ok": True}),
        )
        monkeypatch.setattr(crds, "_settings", lambda: SimpleNamespace(
            crd_group="kubortex.io",
            crd_version="v1alpha1",
            namespace="kubortex-system",
        ))
        async def _api():
            return api
        monkeypatch.setattr(crds, "_api", _api)

        await crds.patch_spec("incidents", "inc-1", {"signals": []})

        call = api.patch_namespaced_custom_object.await_args
        assert call.kwargs["body"] == {"spec": {"signals": []}}

    @pytest.mark.asyncio
    async def test_includes_resource_version_for_optimistic_locking(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        api = SimpleNamespace(
            patch_namespaced_custom_object=AsyncMock(return_value={"ok": True}),
        )
        monkeypatch.setattr(crds, "_settings", lambda: SimpleNamespace(
            crd_group="kubortex.io",
            crd_version="v1alpha1",
            namespace="kubortex-system",
        ))
        async def _api():
            return api
        monkeypatch.setattr(crds, "_api", _api)

        await crds.patch_spec(
            "incidents",
            "inc-1",
            {"signals": []},
            resource_version="42",
        )

        call = api.patch_namespaced_custom_object.await_args
        assert call.kwargs["body"] == {
            "metadata": {"resourceVersion": "42"},
            "spec": {"signals": []},
        }


class TestPatchStatus:
    @pytest.mark.asyncio
    async def test_uses_plain_status_patch_without_resource_version(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        api = SimpleNamespace(
            patch_namespaced_custom_object_status=AsyncMock(return_value={"ok": True}),
        )
        monkeypatch.setattr(crds, "_settings", lambda: SimpleNamespace(
            crd_group="kubortex.io",
            crd_version="v1alpha1",
            namespace="kubortex-system",
        ))
        async def _api():
            return api
        monkeypatch.setattr(crds, "_api", _api)

        await crds.patch_status("incidents", "inc-1", {"phase": "Detected"})

        call = api.patch_namespaced_custom_object_status.await_args
        assert call.kwargs["body"] == {"status": {"phase": "Detected"}}

    @pytest.mark.asyncio
    async def test_includes_resource_version_for_optimistic_locking(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        api = SimpleNamespace(
            patch_namespaced_custom_object_status=AsyncMock(return_value={"ok": True}),
        )
        monkeypatch.setattr(crds, "_settings", lambda: SimpleNamespace(
            crd_group="kubortex.io",
            crd_version="v1alpha1",
            namespace="kubortex-system",
        ))
        async def _api():
            return api
        monkeypatch.setattr(crds, "_api", _api)

        await crds.patch_status(
            "incidents",
            "inc-1",
            {"phase": "Detected"},
            resource_version="42",
        )

        call = api.patch_namespaced_custom_object_status.await_args
        assert call.kwargs["body"] == {
            "metadata": {"resourceVersion": "42"},
            "status": {"phase": "Detected"},
        }
