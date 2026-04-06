"""Core notification projection workflow for Edge.

Uses kubernetes_asyncio watch to observe phase transitions on all Kubortex
CRDs and maps them to notification events for downstream adapters.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

import structlog
from kubernetes_asyncio import client as k8s_client
from kubernetes_asyncio import watch

from kubortex.edge.core.events import (
    ActionExecuted,
    ActionFailed,
    ActionSucceeded,
    ApprovalRejected,
    ApprovalRequired,
    ApprovalTimedOut,
    DomainEvent,
    EscalationTriggered,
    IncidentDetected,
    IncidentFailed,
    IncidentResolved,
    InvestigationCompleted,
    InvestigationStarted,
    RemediationPlanned,
)
from kubortex.shared.config import EdgeSettings
from kubortex.shared.constants import (
    ACTION_EXECUTIONS,
    APPROVAL_REQUESTS,
    INCIDENTS,
    INVESTIGATIONS,
)
from kubortex.shared.kube_clients import get_kubernetes_clients

logger = structlog.get_logger(__name__)
_EVENT_QUEUE_MAXSIZE = 256


class EventProjector:
    """Watch Kubortex CRDs and emit notification events on phase changes."""

    def __init__(self, settings: EdgeSettings | None = None) -> None:
        self._settings = settings or EdgeSettings()
        self._seen_phases: dict[str, str] = {}  # resource uid -> last seen phase
        self._ready = asyncio.Event()

    @property
    def is_ready(self) -> bool:
        """Whether the projector has primed its watch state."""
        return self._ready.is_set()

    async def watch_events(self) -> AsyncIterator[DomainEvent]:
        """Yield notification events from all watched CRD types.

        Events are buffered through a bounded queue so sink-side slowness
        applies backpressure to the resource watches instead of letting
        notification memory usage grow without bound.

        Returns:
            Async iterator of projected notification events.
        """
        clients = await get_kubernetes_clients()
        api = clients.custom_objects
        s = self._settings

        watched_resources = [
            INCIDENTS,
            INVESTIGATIONS,
            APPROVAL_REQUESTS,
            ACTION_EXECUTIONS,
        ]

        queue: asyncio.Queue[DomainEvent] = asyncio.Queue(maxsize=_EVENT_QUEUE_MAXSIZE)

        async def _feed(plural: str, resource_version: str | None) -> None:
            async for event in self._watch_resource(api, s, plural, resource_version):
                await queue.put(event)

        resource_versions = {
            plural: await self._initialize_resource_watch_state(api, s, plural)
            for plural in watched_resources
        }
        feed_tasks = [
            asyncio.create_task(_feed(plural, resource_versions[plural]))
            for plural in watched_resources
        ]
        self._ready.set()

        try:
            while True:
                event = await queue.get()
                yield event
        finally:
            self._ready.clear()
            for t in feed_tasks:
                t.cancel()
            await asyncio.gather(*feed_tasks, return_exceptions=True)

    async def _watch_resource(
        self,
        api: k8s_client.CustomObjectsApi,
        settings: EdgeSettings,
        plural: str,
        resource_version: str | None = None,
    ) -> AsyncIterator[DomainEvent]:
        """Watch one CRD type and yield projected notification events.

        Args:
            api: Kubernetes custom objects client.
            settings: Edge settings with CRD coordinates.
            plural: Resource plural to watch.

        Returns:
            Async iterator of projected notification events.
        """
        current_resource_version = resource_version
        while True:
            try:
                watcher = watch.Watch()
                try:
                    async for event in watcher.stream(
                        api.list_namespaced_custom_object,
                        group=settings.crd_group,
                        version=settings.crd_version,
                        namespace=settings.namespace,
                        plural=plural,
                        resource_version=current_resource_version,
                    ):
                        obj = event.get("object", {})
                        event_type = event.get("type", "")
                        if isinstance(obj, dict):
                            metadata = obj.get("metadata", {})
                            if isinstance(metadata, dict):
                                current_resource_version = (
                                    metadata.get("resourceVersion") or current_resource_version
                                )

                        if event_type == "DELETED":
                            uid = obj.get("metadata", {}).get("uid", "")
                            if uid:
                                self._seen_phases.pop(uid, None)
                            continue

                        if event_type not in ("ADDED", "MODIFIED"):
                            continue

                        domain_event = self._project(plural, obj, event_type=event_type)
                        if domain_event:
                            yield domain_event
                finally:
                    watcher.stop()

            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("watch_error", plural=plural)
                await asyncio.sleep(5)
            current_resource_version = await self._initialize_resource_watch_state(
                api,
                settings,
                plural,
            )

    def _project(
        self,
        plural: str,
        obj: dict[str, Any],
        *,
        event_type: str | None = None,
    ) -> DomainEvent | None:
        """Project a CRD object to a notification event when its phase changes.

        Args:
            plural: Resource plural for the object.
            obj: Kubernetes custom resource object.

        Returns:
            Projected notification event, or ``None`` when no event applies.
        """
        if not isinstance(obj, dict):
            logger.warning("event_projection_skipped", plural=plural, reason="object_not_mapping")
            return None

        metadata = obj.get("metadata")
        if not isinstance(metadata, dict):
            logger.warning("event_projection_skipped", plural=plural, reason="invalid_object_shape")
            return None

        status = obj.get("status")
        if isinstance(status, dict):
            phase = status.get("phase", "")
        elif plural == INCIDENTS and event_type == "ADDED":
            phase = "Detected"
            status = {}
        else:
            logger.warning("event_projection_skipped", plural=plural, reason="invalid_object_shape")
            return None

        uid = metadata.get("uid", "")
        prev_phase = self._seen_phases.get(uid)

        if not phase:
            return None

        if phase == prev_phase:
            return None

        self._seen_phases[uid] = phase
        name = metadata.get("name", "")
        namespace = metadata.get("namespace", "")

        try:
            base = {
                "incidentName": self._resolve_incident_name(plural, obj),
                "namespace": namespace,
                "timestamp": datetime.now(UTC),
                "payload": self._build_payload(plural, obj, phase),
            }
            return self._map_event(plural, phase, base)
        except Exception:
            logger.exception(
                "event_projection_skipped",
                plural=plural,
                resource=name,
            )
            return None

    def _map_event(self, plural: str, phase: str, base: dict[str, Any]) -> DomainEvent | None:
        """Map a resource type and phase to a notification event.

        Args:
            plural: Resource plural.
            phase: Current resource phase.
            base: Shared event payload fields.

        Returns:
            Concrete notification event, or ``None`` when unmapped.
        """
        mapping: dict[tuple[str, str], type[DomainEvent]] = {
            ("incidents", "Detected"): IncidentDetected,
            ("incidents", "RemediationPlanned"): RemediationPlanned,
            ("incidents", "Failed"): IncidentFailed,
            ("incidents", "Resolved"): IncidentResolved,
            ("incidents", "Escalated"): EscalationTriggered,
            ("investigations", "InProgress"): InvestigationStarted,
            ("investigations", "Completed"): InvestigationCompleted,
            ("approvalrequests", "Pending"): ApprovalRequired,
            ("approvalrequests", "Rejected"): ApprovalRejected,
            ("approvalrequests", "TimedOut"): ApprovalTimedOut,
            ("actionexecutions", "Executing"): ActionExecuted,
            ("actionexecutions", "Succeeded"): ActionSucceeded,
            ("actionexecutions", "Failed"): ActionFailed,
            ("actionexecutions", "RolledBack"): ActionFailed,
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
        if not isinstance(spec, dict):
            spec = {}

        incident_ref = spec.get("incidentRef", "")
        if isinstance(incident_ref, str) and incident_ref:
            return incident_ref
        if incident_ref not in ("", None):
            logger.warning(
                "event_projection_skipped",
                plural=plural,
                reason="invalid_incident_ref_shape",
            )

        # Fall back to owner references
        for ref in obj.get("metadata", {}).get("ownerReferences", []):
            if ref.get("kind") == "Incident":
                return ref.get("name", "")

        return "unknown"

    async def _initialize_resource_watch_state(
        self,
        api: k8s_client.CustomObjectsApi,
        settings: EdgeSettings,
        plural: str,
    ) -> str | None:
        """Initialize local watch state to suppress replayed startup notifications.

        Edge first snapshots the current objects and records their phases in
        ``_seen_phases`` before opening the streaming watch. This prevents
        already-existing resources from being re-emitted as fresh events after
        restarts. Later ``DELETED`` events remove cached UIDs again so the
        projector does not retain stale phase entries forever, while the
        returned ``resourceVersion`` lets the watch resume from the same
        snapshot boundary.

        Args:
            api: Kubernetes custom objects client.
            settings: Edge settings with CRD coordinates.
            plural: Resource plural to initialize.

        Returns:
            Resource version for the list response, when present.
        """
        while True:
            try:
                response = await api.list_namespaced_custom_object(
                    group=settings.crd_group,
                    version=settings.crd_version,
                    namespace=settings.namespace,
                    plural=plural,
                )
                items = response.get("items", [])

                for item in items:
                    metadata = item.get("metadata", {})
                    uid = metadata.get("uid", "")
                    phase = (item.get("status") or {}).get("phase", "")
                    if uid and phase:
                        self._seen_phases[uid] = phase
                return (response.get("metadata") or {}).get("resourceVersion")
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("watch_state_init_error", plural=plural)
                await asyncio.sleep(5)

    def _build_payload(self, plural: str, obj: dict[str, Any], phase: str) -> dict[str, Any]:
        """Build a best-effort payload for a projected notification event.

        Args:
            plural: Resource plural being projected.
            obj: Kubernetes custom resource object.
            phase: Current resource phase.

        Returns:
            Payload dict with common and resource-specific notification fields.
        """
        metadata = obj.get("metadata", {})
        spec = obj.get("spec") or {}
        status = obj.get("status") or {}
        if not isinstance(metadata, dict):
            metadata = {}
        if not isinstance(spec, dict):
            spec = {}
        if not isinstance(status, dict):
            status = {}
        payload: dict[str, Any] = {
            "resourceName": metadata.get("name", ""),
            "phase": phase,
        }

        if plural == INCIDENTS:
            payload.update(
                {
                    "summary": spec.get("summary", ""),
                    "severity": spec.get("severity", ""),
                    "categories": [c for c in spec.get("categories", []) if isinstance(c, str)],
                }
            )
            self._add_target_fields(payload, spec.get("targetRef"))
            investigation = status.get("investigation") or {}
            if isinstance(investigation, dict):
                payload.update(
                    {
                        "confidence": investigation.get("confidence"),
                        "hypothesis": investigation.get("hypothesis", ""),
                        "evidenceCount": investigation.get("evidenceCount"),
                        "proposedActionCount": investigation.get("proposedActionCount"),
                    }
                )
            return payload

        if plural == INVESTIGATIONS:
            payload.update(
                {
                    "summary": spec.get("summary", ""),
                    "severity": spec.get("severity", ""),
                    "categories": [c for c in spec.get("categories", []) if isinstance(c, str)],
                }
            )
            self._add_target_fields(payload, spec.get("targetRef"))
            result = status.get("result") or {}
            if isinstance(result, dict):
                payload.update(
                    {
                        "confidence": result.get("confidence"),
                        "hypothesis": result.get("hypothesis", ""),
                        "evidenceCount": len(result.get("evidence", [])),
                        "proposedActionCount": len(result.get("recommendedActions", [])),
                    }
                )
            return payload

        if plural == APPROVAL_REQUESTS:
            action = spec.get("action") or {}
            payload["actionType"] = action.get("type", "")
            payload["remediationPlanRef"] = spec.get("remediationPlanRef", "")
            self._add_target_fields(payload, action.get("target"))
            return payload

        if plural == ACTION_EXECUTIONS:
            action = spec.get("action") or {}
            payload["actionType"] = action.get("type", "")
            payload["remediationPlanRef"] = spec.get("remediationPlanRef", "")
            payload["approvalRequestRef"] = spec.get("approvalRequestRef", "")
            self._add_target_fields(payload, action.get("target"))
            verification = status.get("verification") or {}
            if isinstance(verification, dict):
                payload["improved"] = verification.get("improved")
            return payload

        return payload

    def _add_target_fields(self, payload: dict[str, Any], target: Any) -> None:
        """Copy target reference fields into an event payload when available.

        Args:
            payload: Payload dict to mutate in place.
            target: Candidate target reference object from a CR spec.
        """
        if not isinstance(target, dict):
            return
        payload.update(
            {
                "targetKind": target.get("kind", ""),
                "targetNamespace": target.get("namespace", ""),
                "targetName": target.get("name", ""),
            }
        )
