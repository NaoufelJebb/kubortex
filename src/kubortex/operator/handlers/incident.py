"""Kopf handler for Incident CRD lifecycle transitions.

The operator is the sole lifecycle governor. This handler:
- Matches an AutonomyProfile on creation
- Creates Investigation CRs
- Observes investigation results and creates RemediationPlans
- Manages retry logic and escalatioç
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import kopf
import structlog
from kubernetes_asyncio.client import ApiException

from kubortex.operator.settings import GROUP, VERSION, settings
from kubortex.shared.constants import AUTONOMY_PROFILES, INCIDENTS, INVESTIGATIONS
from kubortex.shared.crds import create_resource, get_resource, patch_status
from kubortex.shared.models import IncidentSpec, IncidentStatus
from kubortex.shared.models.autonomy import AutonomyProfileSpec, AutonomyScope
from kubortex.shared.types import IncidentPhase, InvestigationPhase

logger = structlog.get_logger(__name__)


@kopf.on.create(GROUP, VERSION, INCIDENTS)
async def on_incident_create(
    body: dict[str, Any],
    name: str,
    namespace: str,
    **_: Any,
) -> None:
    """Match policy and create the initial investigation for an incident.

    Args:
        body: Incident resource body.
        name: Incident name.
        namespace: Incident namespace.
    """
    spec = IncidentSpec.model_validate(body.get("spec", {}))
    logger.info("incident_created", name=name, severity=spec.severity, categories=spec.categories)

    # Match AutonomyProfile
    profile_name = await _match_autonomy_profile(spec, namespace)
    if not profile_name:
        await _transition(name, namespace, IncidentPhase.ESCALATED, "No matching AutonomyProfile")
        return

    # Set escalation deadline from profile
    try:
        profile = await get_resource(AUTONOMY_PROFILES, profile_name)
    except ApiException as exc:
        if exc.status == 404:
            logger.warning("autonomy_profile_gone", name=name, profile=profile_name)
            await _transition(name, namespace, IncidentPhase.ESCALATED, "AutonomyProfile not found")
            return
        raise
    parsed_profile = AutonomyProfileSpec.model_validate(profile.get("spec", {}))
    deadline_minutes = parsed_profile.escalation_deadline_minutes
    deadline = datetime.now(UTC) + timedelta(minutes=deadline_minutes)
    max_retries = parsed_profile.max_investigation_retries

    # Create Investigation CR (idempotent: 409 means it already exists from a previous attempt)
    inv_name = f"inv-{name}"
    incident_uid = body.get("metadata", {}).get("uid", "")
    inv_body = _build_investigation(inv_name, name, namespace, spec, uid=incident_uid)
    try:
        await create_resource(INVESTIGATIONS, inv_body)
        await patch_status(
            INVESTIGATIONS,
            inv_name,
            {"phase": InvestigationPhase.PENDING},
        )
    except ApiException as exc:
        if exc.status == 409:
            logger.info("investigation_already_exists", name=name, investigation=inv_name)
        else:
            raise

    await patch_status(
        INCIDENTS,
        name,
        {
            "phase": IncidentPhase.INVESTIGATING,
            "autonomyProfile": profile_name,
            "investigationRef": inv_name,
            "escalationDeadline": deadline.isoformat(),
            "maxRetries": max_retries,
        },
    )
    logger.info("incident_investigating", name=name, investigation=inv_name)


@kopf.on.field(GROUP, VERSION, INCIDENTS, field="status.phase")
async def on_incident_failed(
    body: dict[str, Any],
    name: str,
    namespace: str,
    new: str | None,
    **_: Any,
) -> None:
    """Re-spawn an Investigation when an Incident transitions to Failed.

    Triggered by every ``status.phase`` change; only acts when ``new`` is
    ``Failed`` — all other phases are ignored.

    Reads ``status.retryCount`` and ``status.autonomyProfile`` from the
    incident, re-fetches the AutonomyProfile to compute a fresh escalation
    deadline, then creates a new Investigation named
    ``inv-{incident}-r{retryCount}`` (suffixed to avoid collision with the
    original ``inv-{incident}``). The Incident is transitioned back to
    Investigating with the updated ``investigationRef`` and deadline.

    Args:
        body: Incident resource body.
        name: Incident name.
        namespace: Incident namespace.
        new: New phase value (only acts on ``Failed``).
    """
    if new != IncidentPhase.FAILED:
        return

    status = body.get("status", {})
    retry_count = status.get("retryCount", 0)
    profile_name = status.get("autonomyProfile", "")

    if not profile_name:
        logger.warning("incident_failed_no_profile", name=name)
        await _transition(name, namespace, IncidentPhase.ESCALATED, "No AutonomyProfile on retry")
        return

    try:
        profile = await get_resource(AUTONOMY_PROFILES, profile_name)
    except ApiException as exc:
        if exc.status == 404:
            logger.warning("autonomy_profile_gone_on_retry", name=name, profile=profile_name)
            await _transition(
                name,
                namespace,
                IncidentPhase.ESCALATED,
                "AutonomyProfile not found on retry",
            )
            return
        raise

    parsed_retry_profile = AutonomyProfileSpec.model_validate(profile.get("spec", {}))
    deadline_minutes = parsed_retry_profile.escalation_deadline_minutes
    deadline = datetime.now(UTC) + timedelta(minutes=deadline_minutes)

    spec = IncidentSpec.model_validate(body.get("spec", {}))
    incident_uid = body.get("metadata", {}).get("uid", "")
    inv_name = f"inv-{name}-r{retry_count}"

    # Collect prior attempt context from the previous investigation
    prev_inv_name = status.get("investigationRef", "")
    prior_attempts: list[dict] = []
    if prev_inv_name:
        try:
            prev_inv = await get_resource(INVESTIGATIONS, prev_inv_name)
            prev_result = (prev_inv.get("status") or {}).get("result", {})
            if isinstance(prev_result, dict) and prev_result:
                prior_attempts = [
                    {
                        "hypothesis": prev_result.get("hypothesis", ""),
                        "failureReason": prev_result.get(
                            "escalationReason",
                            "Action failed or rolled back",
                        ),
                    }
                ]
        except ApiException:
            pass

    inv_body = _build_investigation(
        inv_name, name, namespace, spec, uid=incident_uid, prior_attempts=prior_attempts
    )

    try:
        await create_resource(INVESTIGATIONS, inv_body)
        await patch_status(
            INVESTIGATIONS,
            inv_name,
            {"phase": InvestigationPhase.PENDING},
        )
    except ApiException as exc:
        if exc.status == 409:
            logger.info("investigation_already_exists_on_retry", name=name, investigation=inv_name)
        else:
            raise

    await patch_status(
        INCIDENTS,
        name,
        {
            "phase": IncidentPhase.INVESTIGATING,
            "investigationRef": inv_name,
            "escalationDeadline": deadline.isoformat(),
        },
    )
    logger.info(
        "incident_retry_investigating",
        name=name,
        investigation=inv_name,
        attempt=retry_count,
    )


@kopf.timer(GROUP, VERSION, INCIDENTS, interval=settings.escalation_check_interval)
async def check_escalation_deadline(
    body: dict[str, Any],
    name: str,
    namespace: str,
    **_: Any,
) -> None:
    """Escalate an incident after its investigation deadline.

    Args:
        body: Incident resource body.
        name: Incident name.
        namespace: Incident namespace.
    """
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


async def _get_namespace_labels(namespace: str) -> dict[str, str]:
    """Fetch a namespace's labels for label-selector matching.

    Args:
        namespace: Kubernetes namespace name.

    Returns:
        Label dict for the namespace, or empty dict when unreadable.
    """
    from kubernetes_asyncio import client as k8s_client

    try:
        async with k8s_client.ApiClient() as api_client:
            v1 = k8s_client.CoreV1Api(api_client)
            ns_obj = await v1.read_namespace(namespace, _request_timeout=3.0)
        return dict(ns_obj.metadata.labels or {})
    except Exception:
        logger.warning("namespace_labels_unreadable")
        return {}


def _scope_matches(
    scope: AutonomyScope,
    spec: IncidentSpec,
    ns_labels: dict[str, str] | None,
) -> bool:
    """Return True when all scope constraints are satisfied by the incident.

    Args:
        scope: Profile scope to evaluate.
        spec: Incident to match against.
        ns_labels: Labels of the incident's target namespace, or ``None`` when
            the incident has no ``targetRef``.

    Returns:
        Whether the scope matches the incident.
    """
    if scope.severities and spec.severity not in scope.severities:
        return False
    if scope.categories and not any(c in scope.categories for c in spec.categories):
        return False

    ns_sel = scope.namespaces
    target_ns = spec.target_ref.namespace if spec.target_ref else None

    if ns_sel.match_names and (target_ns is None or target_ns not in ns_sel.match_names):
        return False

    if ns_sel.match_labels:
        # Cannot satisfy a label selector with no namespace
        if ns_labels is None:
            return False
        if not ns_sel.match_labels.items() <= ns_labels.items():
            return False

    return True


def _scope_specificity(scope: AutonomyScope) -> int:
    """Score a scope by how many constraints it imposes.

    Higher score = more specific = preferred over catch-all profiles.

    Args:
        scope: Profile scope to score.

    Returns:
        Specificity score (higher is more specific).
    """
    return (
        len(scope.namespaces.match_names)
        + len(scope.namespaces.match_labels)
        + len(scope.severities)
        + len(scope.categories)
    )


async def _match_autonomy_profile(spec: IncidentSpec, namespace: str) -> str | None:
    """Find the most specific matching AutonomyProfile for an incident.

    Each profile's scope is evaluated against the incident's severity,
    category, and target namespace (both by name and by label selector).
    When multiple profiles match, the one with the most constraints wins.
    Ties are broken alphabetically by profile name for determinism.

    Args:
        spec: Incident spec.
        namespace: Namespace to list profiles from.

    Returns:
        Name of the best-matching profile, or ``None`` when none match.
    """
    from kubortex.shared.crds import list_resources

    profiles = await list_resources(AUTONOMY_PROFILES)

    # Fetch namespace labels once — needed only when any profile uses matchLabels
    target_ns = spec.target_ref.namespace if spec.target_ref else None
    ns_labels: dict[str, str] | None = None
    if target_ns:
        ns_labels = await _get_namespace_labels(target_ns)

    candidates: list[tuple[int, str]] = []
    for profile in profiles:
        scope = AutonomyScope.model_validate(profile.get("spec", {}).get("scope", {}))
        if _scope_matches(scope, spec, ns_labels):
            candidates.append((_scope_specificity(scope), profile["metadata"]["name"]))

    if not candidates:
        return None

    # Most specific first; alphabetical tie-break for determinism
    candidates.sort(key=lambda x: (-x[0], x[1]))

    if len(candidates) > 1:
        logger.warning(
            "autonomy_profile_ambiguous",
            candidates=[c[1] for c in candidates],
            selected=candidates[0][1],
        )

    return candidates[0][1]


async def _transition(name: str, namespace: str, phase: IncidentPhase, detail: str) -> None:
    """Update an incident phase and append a timeline entry.

    Args:
        name: Incident name.
        namespace: Incident namespace.
        phase: New incident phase.
        detail: Timeline detail for the transition.
    """
    try:
        resource = await get_resource(INCIDENTS, name)
        existing_timeline = (resource.get("status") or {}).get("timeline", [])
    except ApiException:
        existing_timeline = []
    new_entry = {
        "timestamp": datetime.now(UTC).isoformat(),
        "event": "PhaseTransition",
        "detail": detail,
    }
    await patch_status(
        INCIDENTS,
        name,
        {"phase": phase, "timeline": [*existing_timeline, new_entry]},
    )
    logger.info("incident_transitioned", name=name, phase=phase, detail=detail)


def _build_investigation(
    inv_name: str,
    incident_name: str,
    namespace: str,
    spec: IncidentSpec,
    *,
    uid: str,
    prior_attempts: list[dict] | None = None,
) -> dict[str, Any]:
    """Build an Investigation resource body from an incident.

    Args:
        inv_name: Investigation name.
        incident_name: Parent incident name.
        namespace: Resource namespace.
        spec: Incident spec.
        uid: UID of the parent Incident (required by K8s ownerReferences).
        prior_attempts: Context from previous failed investigation attempts.

    Returns:
        Investigation custom resource body.
    """
    return {
        "apiVersion": f"{GROUP}/{VERSION}",
        "kind": "Investigation",
        "metadata": {
            "name": inv_name,
            "namespace": namespace,
            "ownerReferences": [
                {
                    "apiVersion": f"{GROUP}/{VERSION}",
                    "kind": "Incident",
                    "name": incident_name,
                    "uid": uid,
                    "controller": True,
                }
            ],
        },
        "spec": {
            "incidentRef": incident_name,
            "categories": [c.value for c in spec.categories],
            "severity": spec.severity,
            "summary": spec.summary,
            "targetRef": (spec.target_ref.model_dump() if spec.target_ref else None),
            "signals": [s.model_dump(by_alias=True) for s in spec.signals],
            "priorAttempts": prior_attempts or [],
            "maxIterations": settings.investigation_max_iterations,
            "timeoutSeconds": settings.investigation_timeout_seconds,
        },
    }
