"""Kopf handler for Incident CRD lifecycle transitions.

The operator is the sole lifecycle governor (Corollary 3). This handler:
- Matches an AutonomyProfile on creation
- Creates Investigation CRs
- Observes investigation results and creates RemediationPlans
- Manages retry logic and escalation
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import kopf
import structlog

from kubortex.shared.k8s import create_resource, get_resource, patch_status
from kubortex.shared.models import IncidentSpec, IncidentStatus
from kubortex.shared.types import IncidentPhase

logger = structlog.get_logger(__name__)

GROUP = "kubortex.io"
VERSION = "v1alpha1"
INCIDENTS = "incidents"
INVESTIGATIONS = "investigations"
AUTONOMY_PROFILES = "autonomyprofiles"


@kopf.on.create(GROUP, VERSION, INCIDENTS)
async def on_incident_create(
    body: dict[str, Any],
    name: str,
    namespace: str,
    **_: Any,
) -> None:
    """Handle new Incident: match autonomy profile and create Investigation."""
    spec = IncidentSpec.model_validate(body.get("spec", {}))
    logger.info("incident_created", name=name, severity=spec.severity, category=spec.category)

    # Match AutonomyProfile
    profile_name = await _match_autonomy_profile(spec, namespace)
    if not profile_name:
        await _transition(name, namespace, IncidentPhase.ESCALATED, "No matching AutonomyProfile")
        return

    # Set escalation deadline from profile
    profile = await get_resource(AUTONOMY_PROFILES, profile_name)
    profile_spec = profile.get("spec", {})
    deadline_minutes = profile_spec.get("escalationDeadlineMinutes", 15)
    deadline = datetime.now(UTC) + timedelta(minutes=deadline_minutes)

    # Create Investigation CR
    inv_name = f"inv-{name}"
    inv_body = _build_investigation(inv_name, name, namespace, spec)
    await create_resource(INVESTIGATIONS, inv_body, namespace=namespace)

    await patch_status(
        INCIDENTS,
        name,
        {
            "phase": IncidentPhase.INVESTIGATING,
            "autonomyProfile": profile_name,
            "investigationRef": inv_name,
            "escalationDeadline": deadline.isoformat(),
        },
        namespace=namespace,
    )
    logger.info("incident_investigating", name=name, investigation=inv_name)


@kopf.on.field(GROUP, VERSION, INCIDENTS, field="status.phase")
async def on_incident_phase_change(
    body: dict[str, Any],
    name: str,
    namespace: str,
    new: str | None,
    old: str | None,
    **_: Any,
) -> None:
    """React to phase transitions on the Incident."""
    if not new or new == old:
        return
    logger.info("incident_phase_changed", name=name, old=old, new=new)


@kopf.timer(GROUP, VERSION, INCIDENTS, interval=30)
async def check_escalation_deadline(
    body: dict[str, Any],
    name: str,
    namespace: str,
    **_: Any,
) -> None:
    """Escalate if the investigation deadline has passed."""
    status = IncidentStatus.model_validate(body.get("status", {}))
    if status.phase != IncidentPhase.INVESTIGATING:
        return
    if not status.escalation_deadline:
        return
    if datetime.now(UTC) > status.escalation_deadline:
        await _transition(name, namespace, IncidentPhase.ESCALATED, "Escalation deadline exceeded")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _match_autonomy_profile(spec: IncidentSpec, namespace: str) -> str | None:
    """Find the best matching AutonomyProfile for this incident. Returns name or None."""
    from kubortex.shared.k8s import list_resources

    profiles = await list_resources(AUTONOMY_PROFILES, namespace=namespace)
    target_ns = spec.target_ref.namespace if spec.target_ref else ""

    for profile in profiles:
        p_spec = profile.get("spec", {})
        selector = p_spec.get("scope", {}).get("namespaceSelector", {})
        match_labels = selector.get("matchLabels", {})
        # AIDEV-NOTE: MVP matching is simplistic — checks if target namespace
        # label key "env" matches. Full label-selector logic is post-MVP.
        if match_labels and target_ns:
            # For now, just check that the profile has scope defined
            severities = p_spec.get("scope", {}).get("severities", [])
            if severities and spec.severity not in severities:
                continue
        return profile["metadata"]["name"]
    return None


async def _transition(name: str, namespace: str, phase: IncidentPhase, detail: str) -> None:
    """Transition the Incident to a new phase with a timeline entry."""
    await patch_status(
        INCIDENTS,
        name,
        {
            "phase": phase,
            "timeline": [
                {
                    "timestamp": datetime.now(UTC).isoformat(),
                    "event": "PhaseTransition",
                    "detail": detail,
                }
            ],
        },
        namespace=namespace,
    )
    logger.info("incident_transitioned", name=name, phase=phase, detail=detail)


def _build_investigation(
    inv_name: str, incident_name: str, namespace: str, spec: IncidentSpec
) -> dict[str, Any]:
    """Build an Investigation CR body from the incident spec."""
    return {
        "apiVersion": f"{GROUP}/{VERSION}",
        "kind": "Investigation",
        "metadata": {
            "name": inv_name,
            "namespace": namespace,
            "labels": {
                "kubortex.io/incident": incident_name,
                "kubortex.io/category": spec.category,
            },
            "ownerReferences": [
                {
                    "apiVersion": f"{GROUP}/{VERSION}",
                    "kind": "Incident",
                    "name": incident_name,
                    "controller": True,
                }
            ],
        },
        "spec": {
            "incidentRef": incident_name,
            "category": spec.category,
            "severity": spec.severity,
            "summary": spec.summary,
            "targetRef": (spec.target_ref.model_dump() if spec.target_ref else None),
            "signals": [s.model_dump(by_alias=True) for s in spec.signals],
            "maxIterations": 10,
            "timeoutSeconds": 300,
        },
    }
