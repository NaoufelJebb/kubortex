"""Kopf handler for RemediationPlan CRD lifecycle transitions.

Evaluates proposed actions via the policy engine and creates ApprovalRequests
or ActionExecutions based on policy decisions.
"""

from __future__ import annotations

from typing import Any

import kopf
import structlog

from kubortex.shared.k8s import create_resource, get_resource, patch_status
from kubortex.shared.models.autonomy import AutonomyProfileSpec, BudgetUsage
from kubortex.shared.models.remediation import RemediationPlanSpec
from kubortex.shared.types import (
    ApprovalLevel,
    IncidentPhase,
    RemediationPlanPhase,
)

from ..policy import ActionContext, evaluate_action

logger = structlog.get_logger(__name__)

GROUP = "kubortex.io"
VERSION = "v1alpha1"
REMEDIATION_PLANS = "remediationplans"
APPROVAL_REQUESTS = "approvalrequests"
ACTION_EXECUTIONS = "actionexecutions"
INCIDENTS = "incidents"
AUTONOMY_PROFILES = "autonomyprofiles"


@kopf.on.create(GROUP, VERSION, REMEDIATION_PLANS)
async def on_remediation_plan_create(
    body: dict[str, Any],
    name: str,
    namespace: str,
    **_: Any,
) -> None:
    """Evaluate each action in the plan and create approval/execution CRs."""
    plan_spec = RemediationPlanSpec.model_validate(body.get("spec", {}))
    incident = await get_resource("incidents", plan_spec.incident_ref, namespace=namespace)
    profile_name = (incident.get("status") or {}).get("autonomyProfile", "")

    if not profile_name:
        await patch_status(
            REMEDIATION_PLANS,
            name,
            {"phase": RemediationPlanPhase.REJECTED},
            namespace=namespace,
        )
        return

    profile_resource = await get_resource(AUTONOMY_PROFILES, profile_name)
    profile_spec = AutonomyProfileSpec.model_validate(profile_resource.get("spec", {}))
    budget_usage = BudgetUsage.model_validate(
        (profile_resource.get("status") or {}).get("budgetUsage", {})
    )

    needs_approval = False
    all_denied = True

    for action in plan_spec.actions:
        ctx = ActionContext(
            action_type=action.type,
            severity=plan_spec.confidence,  # type: ignore[arg-type]
            confidence=plan_spec.confidence,
            target_key=f"{action.target.namespace}/{action.target.name}",
        )
        decision = evaluate_action(ctx, profile_spec, budget_usage)

        if not decision.allowed:
            logger.warning("action_denied", action=action.id, reason=decision.deny_reason)
            continue

        all_denied = False
        if decision.approval == ApprovalLevel.REQUIRED:
            needs_approval = True
            await _create_approval_request(name, plan_spec, action, namespace)
        else:
            await _create_action_execution(name, plan_spec, action, namespace)

    if all_denied:
        await patch_status(
            REMEDIATION_PLANS,
            name,
            {"phase": RemediationPlanPhase.REJECTED},
            namespace=namespace,
        )
        await patch_status(
            INCIDENTS,
            plan_spec.incident_ref,
            {"phase": IncidentPhase.ESCALATED},
            namespace=namespace,
        )
    elif needs_approval:
        await patch_status(
            INCIDENTS,
            plan_spec.incident_ref,
            {"phase": IncidentPhase.PENDING_APPROVAL},
            namespace=namespace,
        )


async def _create_approval_request(
    plan_name: str,
    plan_spec: RemediationPlanSpec,
    action: Any,
    namespace: str,
) -> None:
    """Create an ApprovalRequest CR for one action."""
    ar_name = f"ar-req-{plan_spec.incident_ref}-{action.id}"
    ar_body = {
        "apiVersion": f"{GROUP}/{VERSION}",
        "kind": "ApprovalRequest",
        "metadata": {
            "name": ar_name,
            "namespace": namespace,
            "labels": {
                "kubortex.io/incident": plan_spec.incident_ref,
                "kubortex.io/action-type": action.type,
            },
        },
        "spec": {
            "incidentRef": plan_spec.incident_ref,
            "remediationPlanRef": plan_name,
            "action": {
                "id": action.id,
                "type": action.type,
                "target": action.target.model_dump(),
                "parameters": action.parameters,
                "rationale": action.rationale,
                "riskTier": action.risk_tier,
            },
            "investigation": {
                "hypothesis": plan_spec.hypothesis,
                "confidence": plan_spec.confidence,
            },
            "timeoutMinutes": 30,
        },
    }
    await create_resource(APPROVAL_REQUESTS, ar_body, namespace=namespace)
    logger.info("approval_request_created", name=ar_name)


async def _create_action_execution(
    plan_name: str,
    plan_spec: RemediationPlanSpec,
    action: Any,
    namespace: str,
) -> None:
    """Create an ActionExecution CR for an auto-approved action."""
    ae_name = f"ae-{plan_spec.incident_ref}-{action.id}"
    ae_body = {
        "apiVersion": f"{GROUP}/{VERSION}",
        "kind": "ActionExecution",
        "metadata": {
            "name": ae_name,
            "namespace": namespace,
            "labels": {
                "kubortex.io/incident": plan_spec.incident_ref,
                "kubortex.io/action-type": action.type,
            },
        },
        "spec": {
            "incidentRef": plan_spec.incident_ref,
            "remediationPlanRef": plan_name,
            "action": {
                "type": action.type,
                "target": action.target.model_dump(),
                "parameters": action.parameters,
                "riskTier": action.risk_tier,
            },
            "verificationMetric": (
                plan_spec.verification_metric.model_dump(by_alias=True)
                if plan_spec.verification_metric
                else None
            ),
            "rollbackOnRegression": True,
        },
    }
    await create_resource(ACTION_EXECUTIONS, ae_body, namespace=namespace)
    logger.info("action_execution_created", name=ae_name)
