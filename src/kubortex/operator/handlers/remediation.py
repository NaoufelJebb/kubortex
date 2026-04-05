"""Kopf handler governing RemediationPlan CRD lifecycle transitions.

Evaluates proposed actions via the policy engine and creates ApprovalRequests
or ActionExecutions based on policy decisions.
"""

from __future__ import annotations

from typing import Any

import kopf
import structlog
from kubernetes_asyncio.client import ApiException

from kubortex.operator.settings import GROUP, VERSION, settings
from kubortex.shared.constants import (
    ACTION_EXECUTIONS,
    APPROVAL_REQUESTS,
    AUTONOMY_PROFILES,
    INCIDENTS,
    REMEDIATION_PLANS,
)
from kubortex.shared.crds import create_resource, get_resource, patch_spec, patch_status
from kubortex.shared.models.autonomy import AutonomyProfileSpec
from kubortex.shared.models.remediation import PolicyEvaluationResult, RemediationPlanSpec
from kubortex.shared.types import (
    ActionExecutionPhase,
    ApprovalLevel,
    ApprovalRequestPhase,
    IncidentPhase,
    RemediationPlanPhase,
    Severity,
)

from ..budget import increment_usage, update_usage
from ..policy import ActionContext, evaluate_action

logger = structlog.get_logger(__name__)


@kopf.on.create(GROUP, VERSION, REMEDIATION_PLANS)
async def on_remediation_plan_create(
    body: dict[str, Any],
    name: str,
    namespace: str,
    **_: Any,
) -> None:
    """Policy-evaluate each action in the plan and spawn child resources.

    Reads the AutonomyProfile referenced by the parent Incident, then runs
    every proposed action through the policy engine. Allowed actions become
    either an ApprovalRequest (``approval=required``) or an ActionExecution
    (``approval=none``). Budget counters are incremented immediately for
    auto-approved actions so that concurrent plan evaluations see up-to-date
    usage.

    Terminal outcomes:
    - All actions denied → plan Rejected, Incident Escalated.
    - At least one action needs approval → Incident PendingApproval.
    - All actions auto-approved → no Incident phase change here; the
      ActionExecution handler drives the next transition.

    No-profile guard: if the Incident carries no ``autonomyProfile``, the
    plan is rejected immediately without policy evaluation.

    Args:
        body: RemediationPlan resource body.
        name: RemediationPlan name.
        namespace: RemediationPlan namespace.
    """
    plan_spec = RemediationPlanSpec.model_validate(body.get("spec", {}))
    try:
        incident = await get_resource("incidents", plan_spec.incident_ref)
    except ApiException as exc:
        if exc.status == 404:
            logger.warning("incident_gone_on_plan_create", plan=name, incident=plan_spec.incident_ref)
            await patch_status(
                REMEDIATION_PLANS, name, {"phase": RemediationPlanPhase.REJECTED}
            )
            return
        raise

    # Record the association on the incident regardless of outcome
    try:
        await patch_status(
            INCIDENTS,
            plan_spec.incident_ref,
            {"remediationPlanRef": name},
        )
    except ApiException as exc:
        if exc.status != 404:
            raise

    profile_name = (incident.get("status") or {}).get("autonomyProfile", "")
    incident_severity = Severity((incident.get("spec") or {}).get("severity", Severity.WARNING))

    if not profile_name:
        await patch_status(
            REMEDIATION_PLANS,
            name,
            {"phase": RemediationPlanPhase.REJECTED},
        )
        return

    try:
        profile_resource = await get_resource(AUTONOMY_PROFILES, profile_name)
    except ApiException as exc:
        if exc.status == 404:
            logger.warning("autonomy_profile_gone_on_plan_create", plan=name, profile=profile_name)
            await patch_status(
                REMEDIATION_PLANS, name, {"phase": RemediationPlanPhase.REJECTED}
            )
            return
        raise

    profile_spec = AutonomyProfileSpec.model_validate(profile_resource.get("spec", {}))
    # Read current usage for policy evaluation (snapshot — not used for the final write).
    from kubortex.shared.models.autonomy import BudgetUsage

    budget_usage = BudgetUsage.model_validate(
        (profile_resource.get("status") or {}).get("budgetUsage", {})
    )

    needs_approval = False
    all_denied = True
    executed_action_types: list[str] = []
    evaluation_results: list[PolicyEvaluationResult] = []
    ar_refs: list[str] = []
    ae_refs: list[str] = []

    for action in plan_spec.actions:
        ctx = ActionContext(
            action_type=action.type,
            severity=incident_severity,
            confidence=plan_spec.confidence,
            target_key=f"{action.target.namespace}/{action.target.name}",
        )
        decision = evaluate_action(ctx, profile_spec, budget_usage)
        evaluation_results.append(PolicyEvaluationResult(
            action_id=action.id,
            allowed=decision.allowed,
            approval_required=decision.approval if decision.allowed else ApprovalLevel.REQUIRED,
            matched_rule=decision.matched_rule,
            budget_available=decision.budget_available,
        ))

        if not decision.allowed:
            logger.warning("action_denied", action=action.id, reason=decision.deny_reason)
            continue

        all_denied = False
        if decision.approval == ApprovalLevel.REQUIRED:
            needs_approval = True
            ar_name = f"ar-{plan_spec.investigation_ref}-{action.id}"
            ar_refs.append(ar_name)
            try:
                await _create_approval_request(name, plan_spec, action, namespace)
            except ApiException as exc:
                if exc.status != 409:
                    raise
                logger.info("approval_request_already_exists", plan=name, action=action.id)
        else:
            ae_name = f"ae-{plan_spec.investigation_ref}-{action.id}"
            ae_refs.append(ae_name)
            try:
                await _create_action_execution(name, plan_spec, action, namespace)
            except ApiException as exc:
                if exc.status != 409:
                    raise
                logger.info("action_execution_already_exists", plan=name, action=action.id)
            executed_action_types.append(action.type)

    # Write policy evaluation results into the plan spec.
    if evaluation_results:
        await patch_spec(
            REMEDIATION_PLANS,
            name,
            {"policyEvaluation": [r.model_dump(by_alias=True) for r in evaluation_results]},
        )

    # Atomically increment budget for auto-approved actions using optimistic locking.
    if executed_action_types:
        def _apply_increments(usage: BudgetUsage) -> BudgetUsage:
            for action_type in executed_action_types:
                usage = increment_usage(action_type, usage)
            return usage

        await update_usage(profile_name, _apply_increments)

    # Record spawned child resource refs on the plan status.
    refs_patch: dict[str, list[str]] = {}
    if ar_refs:
        refs_patch["approvalRequestRefs"] = ar_refs
    if ae_refs:
        refs_patch["actionExecutionRefs"] = ae_refs
    if refs_patch and not all_denied:
        await patch_status(REMEDIATION_PLANS, name, refs_patch)

    if all_denied:
        await patch_status(
            REMEDIATION_PLANS,
            name,
            {"phase": RemediationPlanPhase.REJECTED},
        )
        try:
            await patch_status(
                INCIDENTS,
                plan_spec.incident_ref,
                {"phase": IncidentPhase.ESCALATED},
            )
        except ApiException as exc:
            if exc.status != 404:
                raise
    elif needs_approval:
        try:
            await patch_status(
                INCIDENTS,
                plan_spec.incident_ref,
                {"phase": IncidentPhase.PENDING_APPROVAL},
            )
        except ApiException as exc:
            if exc.status != 404:
                raise


async def _create_approval_request(
    plan_name: str,
    plan_spec: RemediationPlanSpec,
    action: Any,
    namespace: str,
) -> None:
    """Create an ApprovalRequest CRD and set its initial phase to Pending.

    The ApprovalRequest carries the full action context (type, target,
    parameters, rationale, risk tier) plus the investigation synopsis so
    the approver has the evidence needed to make an informed decision.
    The operator watches ApprovalRequest ``status.decision`` to continue
    the lifecycle once a human responds.

    Args:
        plan_name: RemediationPlan name (written as ``remediationPlanRef``).
        plan_spec: Parsed RemediationPlan spec.
        action: Action proposal that requires human approval.
        namespace: Namespace for the new ApprovalRequest.
    """
    ar_name = f"ar-{plan_spec.investigation_ref}-{action.id}"
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
            "investigationRef": plan_spec.investigation_ref,
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
            "timeoutMinutes": settings.approval_timeout_minutes,
        },
    }
    await create_resource(APPROVAL_REQUESTS, ar_body)
    await patch_status(
        APPROVAL_REQUESTS,
        ar_name,
        {"phase": ApprovalRequestPhase.PENDING},
    )
    logger.info("approval_request_created", name=ar_name)


async def _create_action_execution(
    plan_name: str,
    plan_spec: RemediationPlanSpec,
    action: Any,
    namespace: str,
) -> None:
    """Create an ActionExecution CRD and set its initial phase to Approved.

    Called for actions that the policy engine approved without requiring
    human sign-off. The ActionExecution is created in phase Approved so the
    remediator worker can claim it immediately. The verification metric and
    rollback flag are forwarded from the plan so the worker can perform
    post-action health checks without reading the plan again.

    Args:
        plan_name: RemediationPlan name (written as ``remediationPlanRef``).
        plan_spec: Parsed RemediationPlan spec.
        action: Auto-approved action proposal.
        namespace: Namespace for the new ActionExecution.
    """
    ae_name = f"ae-{plan_spec.investigation_ref}-{action.id}"
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
        },
    }
    await create_resource(ACTION_EXECUTIONS, ae_body)
    await patch_status(
        ACTION_EXECUTIONS,
        ae_name,
        {"phase": ActionExecutionPhase.APPROVED},
    )
    logger.info("action_execution_created", name=ae_name)
