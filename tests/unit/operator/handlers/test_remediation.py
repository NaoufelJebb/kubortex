"""Unit tests for the RemediationPlan handler (policy dispatch)."""

from __future__ import annotations

from kubortex.operator.handlers.remediation import (
    _create_action_execution,
    _create_approval_request,
    on_remediation_plan_create,
)
from kubortex.operator.settings import GROUP, VERSION, settings
from kubortex.shared.models.remediation import ActionProposal, RemediationPlanSpec
from kubortex.shared.types import IncidentPhase, RemediationPlanPhase

from ..conftest import make_action_proposal, make_incident_body, make_remediation_plan_body

NS = "kubortex-system"


def _allow_profile_resource(
    action_type: str = "restart-pod",
    approval: str = "none",
    auto_remediate: float = 0.5,
    escalate: float = 0.3,
) -> dict:
    return {
        "metadata": {"name": "allow-profile"},
        "spec": {
            "autonomyRules": [{"actions": [action_type], "approval": approval}],
            "confidenceThresholds": {
                "autoRemediate": auto_remediate,
                "propose": escalate,
                "escalate": escalate,
            },
        },
        "status": {"budgetUsage": {}},
    }


def _deny_profile_resource() -> dict:
    return {"metadata": {"name": "deny-profile"}, "spec": {}, "status": {"budgetUsage": {}}}


# ---------------------------------------------------------------------------
# on_remediation_plan_create
# ---------------------------------------------------------------------------


class TestOnRemediationPlanCreate:
    async def test_no_autonomy_profile_rejects_plan(self, mock_k8s) -> None:
        mock_k8s["get_resource"].return_value = make_incident_body(profile=None)
        body = make_remediation_plan_body()
        await on_remediation_plan_create(body=body, name="rp-1", namespace=NS)

        calls = mock_k8s["patch_status"].call_args_list
        plan_call = next(c for c in calls if c.args[0] == "remediationplans")
        assert plan_call.args[2]["phase"] == RemediationPlanPhase.REJECTED
        mock_k8s["create_resource"].assert_not_awaited()

    async def test_all_denied_rejects_plan_and_escalates_incident(self, mock_k8s) -> None:
        incident = make_incident_body(profile="deny-profile")
        profile = _deny_profile_resource()
        mock_k8s["get_resource"].side_effect = [incident, profile]
        body = make_remediation_plan_body()
        await on_remediation_plan_create(body=body, name="rp-1", namespace=NS)

        calls = mock_k8s["patch_status"].call_args_list
        plan_call = next(c for c in calls if c.args[0] == "remediationplans")
        assert plan_call.args[2]["phase"] == RemediationPlanPhase.REJECTED
        incident_phase_call = next(
            c for c in calls if c.args[0] == "incidents" and "phase" in c.args[2]
        )
        assert incident_phase_call.args[2]["phase"] == IncidentPhase.ESCALATED
        mock_k8s["create_resource"].assert_not_awaited()

    async def test_needs_approval_creates_ar_and_sets_incident_pending(self, mock_k8s) -> None:
        incident = make_incident_body(profile="allow-profile")
        profile = _allow_profile_resource(approval="required", escalate=0.3)
        mock_k8s["get_resource"].side_effect = [incident, profile]
        body = make_remediation_plan_body(confidence=0.9)
        await on_remediation_plan_create(body=body, name="rp-1", namespace=NS)

        mock_k8s["create_resource"].assert_awaited_once()
        created_plural = mock_k8s["create_resource"].call_args.args[0]
        assert created_plural == "approvalrequests"

        calls = mock_k8s["patch_status"].call_args_list
        incident_phase_call = next(
            c for c in calls if c.args[0] == "incidents" and "phase" in c.args[2]
        )
        assert incident_phase_call.args[2]["phase"] == IncidentPhase.PENDING_APPROVAL

    async def test_all_auto_approved_creates_ae_no_incident_phase_change(self, mock_k8s) -> None:
        incident = make_incident_body(profile="allow-profile")
        profile = _allow_profile_resource(approval="none", auto_remediate=0.5, escalate=0.3)
        mock_k8s["get_resource"].side_effect = [incident, profile]
        body = make_remediation_plan_body(confidence=0.9)
        await on_remediation_plan_create(body=body, name="rp-1", namespace=NS)

        mock_k8s["create_resource"].assert_awaited_once()
        created_plural = mock_k8s["create_resource"].call_args.args[0]
        assert created_plural == "actionexecutions"

        calls = mock_k8s["patch_status"].call_args_list
        incident_phase_calls = [
            c for c in calls if c.args[0] == "incidents" and "phase" in c.args[2]
        ]
        assert len(incident_phase_calls) == 0

    async def test_mixed_actions_correct_resource_counts(self, mock_k8s) -> None:
        incident = make_incident_body(profile="mixed-profile")
        # Profile: restart-pod auto-approved, rollback-deployment needs approval
        profile = {
            "metadata": {"name": "mixed-profile"},
            "spec": {
                "autonomyRules": [
                    {"actions": ["restart-pod"], "approval": "none"},
                    {"actions": ["rollback-deployment"], "approval": "required"},
                ],
                "confidenceThresholds": {
                    "autoRemediate": 0.5,
                    "propose": 0.3,
                    "escalate": 0.3,
                },
            },
            "status": {"budgetUsage": {}},
        }
        mock_k8s["get_resource"].side_effect = [incident, profile]
        body = make_remediation_plan_body(
            confidence=0.9,
            actions=[
                make_action_proposal("a1", "restart-pod"),
                make_action_proposal("a2", "rollback-deployment"),
            ],
        )
        await on_remediation_plan_create(body=body, name="rp-1", namespace=NS)

        assert mock_k8s["create_resource"].await_count == 2
        created_plurals = {c.args[0] for c in mock_k8s["create_resource"].call_args_list}
        assert "actionexecutions" in created_plurals
        assert "approvalrequests" in created_plurals


# ---------------------------------------------------------------------------
# _create_approval_request
# ---------------------------------------------------------------------------


class TestCreateApprovalRequest:
    def _plan_spec_and_action(self):
        plan_spec = RemediationPlanSpec.model_validate(
            {
                "incidentRef": "inc-1",
                "investigationRef": "inv-1",
                "hypothesis": "H",
                "confidence": 0.9,
                "actions": [make_action_proposal()],
            }
        )
        action = ActionProposal.model_validate(make_action_proposal())
        return plan_spec, action

    async def test_creates_approval_request_resource(self, mock_k8s) -> None:
        plan_spec, action = self._plan_spec_and_action()
        await _create_approval_request("rp-1", plan_spec, action, NS)
        mock_k8s["create_resource"].assert_awaited_once()

    async def test_ar_body_kind_and_api_version(self, mock_k8s) -> None:
        plan_spec, action = self._plan_spec_and_action()
        await _create_approval_request("rp-1", plan_spec, action, NS)
        body = mock_k8s["create_resource"].call_args.args[1]
        assert body["kind"] == "ApprovalRequest"
        assert body["apiVersion"] == f"{GROUP}/{VERSION}"

    async def test_ar_body_timeout_from_settings(self, mock_k8s) -> None:
        plan_spec, action = self._plan_spec_and_action()
        await _create_approval_request("rp-1", plan_spec, action, NS)
        body = mock_k8s["create_resource"].call_args.args[1]
        assert body["spec"]["timeoutMinutes"] == settings.approval_timeout_minutes

    async def test_ar_body_labels_include_incident(self, mock_k8s) -> None:
        plan_spec, action = self._plan_spec_and_action()
        await _create_approval_request("rp-1", plan_spec, action, NS)
        body = mock_k8s["create_resource"].call_args.args[1]
        assert body["metadata"]["labels"]["kubortex.io/incident"] == "inc-1"


# ---------------------------------------------------------------------------
# _create_action_execution
# ---------------------------------------------------------------------------


class TestCreateActionExecution:
    def _plan_spec_and_action(self, with_verification: bool = False):
        spec_dict: dict = {
            "incidentRef": "inc-1",
            "investigationRef": "inv-1",
            "hypothesis": "H",
            "confidence": 0.9,
            "actions": [make_action_proposal()],
        }
        if with_verification:
            spec_dict["verificationMetric"] = {
                "promql": "rate(errors[2m])",
                "successThreshold": 0.01,
                "checkDelaySeconds": 60,
            }
        plan_spec = RemediationPlanSpec.model_validate(spec_dict)
        action = ActionProposal.model_validate(make_action_proposal())
        return plan_spec, action

    async def test_creates_action_execution_resource(self, mock_k8s) -> None:
        plan_spec, action = self._plan_spec_and_action()
        await _create_action_execution("rp-1", plan_spec, action, NS)
        mock_k8s["create_resource"].assert_awaited_once()
        assert mock_k8s["create_resource"].call_args.args[0] == "actionexecutions"

    async def test_ae_body_kind_and_rollback_flag(self, mock_k8s) -> None:
        plan_spec, action = self._plan_spec_and_action()
        await _create_action_execution("rp-1", plan_spec, action, NS)
        body = mock_k8s["create_resource"].call_args.args[1]
        assert body["kind"] == "ActionExecution"
        assert body["spec"]["rollbackOnRegression"] is True

    async def test_ae_body_verification_metric_when_present(self, mock_k8s) -> None:
        plan_spec, action = self._plan_spec_and_action(with_verification=True)
        await _create_action_execution("rp-1", plan_spec, action, NS)
        body = mock_k8s["create_resource"].call_args.args[1]
        assert body["spec"]["verificationMetric"] is not None
        assert body["spec"]["verificationMetric"]["promql"] == "rate(errors[2m])"

    async def test_ae_body_verification_metric_none_when_absent(self, mock_k8s) -> None:
        plan_spec, action = self._plan_spec_and_action(with_verification=False)
        await _create_action_execution("rp-1", plan_spec, action, NS)
        body = mock_k8s["create_resource"].call_args.args[1]
        assert body["spec"]["verificationMetric"] is None
