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

from kubortex.shared.config import EdgeSettings
from kubortex.shared.constants import (
    ACTION_EXECUTIONS,
    APPROVAL_REQUESTS,
    INCIDENTS,
    INVESTIGATIONS,
    REMEDIATION_PLANS,
)

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
    """Watch Kubortex CRDs and emit domain events on phase changes."""

    def __init__(self, settings: EdgeSettings | None = None) -> None:
        self._settings = settings or EdgeSettings()
        self._seen_phases: dict[str, str] = {}  # resource uid -> last seen phase

    async def watch_events(self) -> AsyncIterator[DomainEvent]:
        """Yield domain events from all watched CRD types.

        Returns:
            Async iterator of projected domain events.
        """
        api = k8s_client.CustomObjectsApi()
        s = self._settings

        # Watch all CRD types concurrently
        tasks = [
            self._watch_resource(api, s, INCIDENTS),
            self._watch_resource(api, s, INVESTIGATIONS),
            self._watch_resource(api, s, REMEDIATION_PLANS),
            self._watch_resource(api, s, APPROVAL_REQUESTS),
            self._watch_resource(api, s, ACTION_EXECUTIONS),
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
        settings: EdgeSettings,
        plural: str,
    ) -> AsyncIterator[DomainEvent]:
        """Watch one CRD type and yield projected events.

        Args:
            api: Kubernetes custom objects client.
            settings: Edge settings with CRD coordinates.
            plural: Resource plural to watch.

        Returns:
            Async iterator of projected domain events.
        """
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
        """Project a CRD object to a domain event when its phase changes.

        Args:
            plural: Resource plural for the object.
            obj: Kubernetes custom resource object.

        Returns:
            Projected domain event, or ``None`` when no event applies.
        """
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
        """Map a resource type and phase to a domain event.

        Args:
            plural: Resource plural.
            phase: Current resource phase.
            base: Shared event payload fields.

        Returns:
            Concrete domain event, or ``None`` when unmapped.
        """
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
        """Resolve the incident name for a resource.

        Args:
            plural: Resource plural.
            obj: Kubernetes custom resource object.

        Returns:
            Incident name for the resource.
        """
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
