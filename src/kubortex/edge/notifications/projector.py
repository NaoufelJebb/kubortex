"""Event projector — watches CRD status changes and emits domain events.

Uses kubernetes_asyncio watch to observe phase transitions on all Kubortex
CRDs and maps them to domain events for downstream notification sinks.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

import structlog
from kubernetes_asyncio import client as k8s_client
from kubernetes_asyncio import watch

from kubortex.shared.config import KubortexSettings

from .events import (
    ActionExecuted,
    ActionFailed,
    ActionSucceeded,
    ApprovalRequired,
    DomainEvent,
    EscalationTriggered,
    IncidentDetected,
    IncidentResolved,
    InvestigationCompleted,
    InvestigationStarted,
    RemediationPlanned,
)

logger = structlog.get_logger(__name__)


class EventProjector:
    """Watches Kubortex CRDs and yields domain events on phase transitions."""

    def __init__(self, settings: KubortexSettings | None = None) -> None:
        self._settings = settings or KubortexSettings()
        self._seen_phases: dict[str, str] = {}  # resource uid -> last seen phase

    async def watch_events(self) -> AsyncIterator[DomainEvent]:
        """Yield domain events as CRD phases change."""
        api = k8s_client.CustomObjectsApi()
        s = self._settings

        # Watch all CRD types concurrently
        tasks = [
            self._watch_resource(api, s, "incidents"),
            self._watch_resource(api, s, "investigations"),
            self._watch_resource(api, s, "remediationplans"),
            self._watch_resource(api, s, "approvalrequests"),
            self._watch_resource(api, s, "actionexecutions"),
        ]

        queue: asyncio.Queue[DomainEvent] = asyncio.Queue()

        async def _feed(coro: Any) -> None:
            async for event in coro:
                await queue.put(event)

        feed_tasks = [asyncio.create_task(_feed(t)) for t in tasks]

        try:
            while True:
                event = await queue.get()
                yield event
        finally:
            for t in feed_tasks:
                t.cancel()

    async def _watch_resource(
        self,
        api: k8s_client.CustomObjectsApi,
        settings: KubortexSettings,
        plural: str,
    ) -> AsyncIterator[DomainEvent]:
        """Watch a single CRD type and yield domain events on phase changes."""
        w = watch.Watch()

        while True:
            try:
                async for event in w.stream(
                    api.list_namespaced_custom_object,
                    group=settings.crd_group,
                    version=settings.crd_version,
                    namespace=settings.namespace,
                    plural=plural,
                ):
                    obj = event.get("object", {})
                    event_type = event.get("type", "")

                    if event_type not in ("ADDED", "MODIFIED"):
                        continue

                    domain_event = self._project(plural, obj)
                    if domain_event:
                        yield domain_event

            except Exception:
                logger.exception("watch_error", plural=plural)
                await asyncio.sleep(5)

    def _project(self, plural: str, obj: dict[str, Any]) -> DomainEvent | None:
        """Map a CRD object to a domain event if phase changed."""
        uid = obj.get("metadata", {}).get("uid", "")
        status = obj.get("status") or {}
        phase = status.get("phase", "")
        prev_phase = self._seen_phases.get(uid)

        if phase == prev_phase:
            return None

        self._seen_phases[uid] = phase
        name = obj.get("metadata", {}).get("name", "")
        namespace = obj.get("metadata", {}).get("namespace", "")
        now = datetime.now(UTC)

        incident_name = self._resolve_incident_name(plural, obj)

        base = {
            "incidentName": incident_name,
            "namespace": namespace,
            "timestamp": now,
            "payload": {"resourceName": name, "phase": phase},
        }

        return self._map_event(plural, phase, base)

    def _map_event(self, plural: str, phase: str, base: dict[str, Any]) -> DomainEvent | None:
        """Map (resource plural, phase) to a concrete domain event class."""
        mapping: dict[tuple[str, str], type[DomainEvent]] = {
            ("incidents", "Detected"): IncidentDetected,
            ("incidents", "Resolved"): IncidentResolved,
            ("incidents", "Escalated"): EscalationTriggered,
            ("investigations", "InProgress"): InvestigationStarted,
            ("investigations", "Completed"): InvestigationCompleted,
            ("remediationplans", "PendingApproval"): RemediationPlanned,
            ("approvalrequests", "Pending"): ApprovalRequired,
            ("actionexecutions", "Executing"): ActionExecuted,
            ("actionexecutions", "Succeeded"): ActionSucceeded,
            ("actionexecutions", "Failed"): ActionFailed,
        }

        cls = mapping.get((plural, phase))
        if cls is None:
            return None

        return cls(**base)

    def _resolve_incident_name(self, plural: str, obj: dict[str, Any]) -> str:
        """Extract the incident name from the resource or its ownerRefs."""
        if plural == "incidents":
            return obj.get("metadata", {}).get("name", "")

        spec = obj.get("spec", {})

        # Investigation, RemediationPlan, etc. reference incident via spec
        incident_ref = spec.get("incidentRef", {})
        if incident_ref.get("name"):
            return incident_ref["name"]

        # Fall back to owner references
        for ref in obj.get("metadata", {}).get("ownerReferences", []):
            if ref.get("kind") == "Incident":
                return ref.get("name", "")

        return "unknown"
