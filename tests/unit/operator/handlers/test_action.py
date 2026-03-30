"""Unit tests for the ActionExecution handler (execution governance)."""

from __future__ import annotations

from kubortex.operator.handlers.action import (
    _handle_failure,
    on_action_claimed,
    on_action_result,
)
from kubortex.operator.settings import settings
from kubortex.shared.types import ActionExecutionPhase, IncidentPhase

from ..conftest import make_action_execution_body, make_incident_body

NS = "kubortex-system"


# ---------------------------------------------------------------------------
# on_action_claimed
# ---------------------------------------------------------------------------


class TestOnActionClaimed:
    async def test_new_is_none_skips(self, mock_k8s) -> None:
        body = make_action_execution_body(phase="Approved")
        await on_action_claimed(body=body, name="ae-1", namespace=NS, new=None, old=None)
        mock_k8s["patch_status"].assert_not_awaited()

    async def test_old_already_set_skips(self, mock_k8s) -> None:
        body = make_action_execution_body(phase="Approved")
        await on_action_claimed(
            body=body, name="ae-1", namespace=NS, new="worker", old="worker"
        )
        mock_k8s["patch_status"].assert_not_awaited()

    async def test_non_approved_phase_skips(self, mock_k8s) -> None:
        body = make_action_execution_body(phase="Executing")
        await on_action_claimed(body=body, name="ae-1", namespace=NS, new="worker", old=None)
        mock_k8s["patch_status"].assert_not_awaited()

    async def test_valid_claim_patches_executing(self, mock_k8s) -> None:
        body = make_action_execution_body(phase="Approved")
        await on_action_claimed(body=body, name="ae-1", namespace=NS, new="worker-1", old=None)
        mock_k8s["patch_status"].assert_awaited_once()
        plural, name, status_patch = mock_k8s["patch_status"].call_args.args
        assert plural == "actionexecutions"
        assert name == "ae-1"
        assert status_patch["phase"] == ActionExecutionPhase.EXECUTING


# ---------------------------------------------------------------------------
# on_action_result
# ---------------------------------------------------------------------------


class TestOnActionResult:
    async def test_no_new_result_skips(self, mock_k8s) -> None:
        body = make_action_execution_body(phase="Executing")
        await on_action_result(body=body, name="ae-1", namespace=NS, new=None)
        mock_k8s["patch_status"].assert_not_awaited()

    async def test_non_executing_phase_skips(self, mock_k8s) -> None:
        body = make_action_execution_body(phase="Approved")
        await on_action_result(body=body, name="ae-1", namespace=NS, new="done")
        mock_k8s["patch_status"].assert_not_awaited()

    async def test_error_patches_ae_failed(self, mock_k8s) -> None:
        mock_k8s["get_resource"].return_value = make_incident_body(retry_count=0, max_retries=2)
        body = make_action_execution_body(
            phase="Executing", incident_ref="inc-1", error="connection refused"
        )
        await on_action_result(body=body, name="ae-1", namespace=NS, new="done")

        calls = mock_k8s["patch_status"].call_args_list
        ae_call = next(c for c in calls if c.args[0] == "actionexecutions")
        assert ae_call.args[2]["phase"] == ActionExecutionPhase.FAILED

    async def test_rollback_triggered_patches_ae_rolled_back(self, mock_k8s) -> None:
        mock_k8s["get_resource"].return_value = make_incident_body(retry_count=0, max_retries=2)
        body = make_action_execution_body(
            phase="Executing", incident_ref="inc-1", rollback_triggered=True
        )
        await on_action_result(body=body, name="ae-1", namespace=NS, new="done")

        calls = mock_k8s["patch_status"].call_args_list
        ae_call = next(c for c in calls if c.args[0] == "actionexecutions")
        assert ae_call.args[2]["phase"] == ActionExecutionPhase.ROLLED_BACK

    async def test_success_patches_ae_succeeded_and_incident_resolved(self, mock_k8s) -> None:
        mock_k8s["get_resource"].return_value = make_incident_body(profile="allow-profile")
        body = make_action_execution_body(
            phase="Executing", incident_ref="inc-1", improved=True
        )
        await on_action_result(body=body, name="ae-1", namespace=NS, new="done")

        calls = mock_k8s["patch_status"].call_args_list
        ae_call = next(c for c in calls if c.args[0] == "actionexecutions")
        assert ae_call.args[2]["phase"] == ActionExecutionPhase.SUCCEEDED

        incident_call = next(c for c in calls if c.args[0] == "incidents")
        assert incident_call.args[2]["phase"] == IncidentPhase.RESOLVED

    async def test_success_ae_includes_resolved_at(self, mock_k8s) -> None:
        mock_k8s["get_resource"].return_value = make_incident_body(profile="allow-profile")
        body = make_action_execution_body(phase="Executing", incident_ref="inc-1", improved=True)
        await on_action_result(body=body, name="ae-1", namespace=NS, new="done")

        calls = mock_k8s["patch_status"].call_args_list
        ae_call = next(c for c in calls if c.args[0] == "actionexecutions")
        assert "resolvedAt" in ae_call.args[2]

    async def test_success_decrements_budget(self, mock_k8s) -> None:
        mock_k8s["get_resource"].return_value = make_incident_body(profile="allow-profile")
        body = make_action_execution_body(phase="Executing", incident_ref="inc-1", improved=True)
        await on_action_result(body=body, name="ae-1", namespace=NS, new="done")

        mock_k8s["load_usage"].assert_awaited_once()
        mock_k8s["decrement_active"].assert_called_once()
        mock_k8s["persist_usage"].assert_awaited_once()

    async def test_success_no_budget_decrement_when_no_profile(self, mock_k8s) -> None:
        mock_k8s["get_resource"].return_value = make_incident_body(profile=None)
        body = make_action_execution_body(phase="Executing", incident_ref="inc-1", improved=True)
        await on_action_result(body=body, name="ae-1", namespace=NS, new="done")

        mock_k8s["decrement_active"].assert_not_called()

    async def test_success_no_incident_ref_no_incident_patch(self, mock_k8s) -> None:
        body = make_action_execution_body(phase="Executing", incident_ref="")
        body["spec"]["incidentRef"] = ""
        await on_action_result(body=body, name="ae-1", namespace=NS, new="done")

        calls = mock_k8s["patch_status"].call_args_list
        incident_calls = [c for c in calls if c.args[0] == "incidents"]
        assert len(incident_calls) == 0


# ---------------------------------------------------------------------------
# _handle_failure
# ---------------------------------------------------------------------------


class TestHandleFailure:
    async def test_under_max_retries_transitions_to_failed(self, mock_k8s) -> None:
        mock_k8s["get_resource"].return_value = make_incident_body(retry_count=0, max_retries=2)
        await _handle_failure("inc-1", NS)

        calls = mock_k8s["patch_status"].call_args_list
        incident_call = next(c for c in calls if c.args[0] == "incidents")
        assert incident_call.args[2]["phase"] == IncidentPhase.FAILED
        assert incident_call.args[2]["retryCount"] == 1

    async def test_failure_decrements_budget(self, mock_k8s) -> None:
        mock_k8s["get_resource"].return_value = make_incident_body(
            retry_count=0, max_retries=2, profile="allow-profile"
        )
        await _handle_failure("inc-1", NS)

        mock_k8s["load_usage"].assert_awaited_once()
        mock_k8s["decrement_active"].assert_called_once()
        mock_k8s["persist_usage"].assert_awaited_once()

    async def test_failure_no_budget_decrement_when_no_profile(self, mock_k8s) -> None:
        mock_k8s["get_resource"].return_value = make_incident_body(
            retry_count=0, max_retries=2, profile=None
        )
        await _handle_failure("inc-1", NS)

        mock_k8s["decrement_active"].assert_not_called()

    async def test_at_max_retries_escalates(self, mock_k8s) -> None:
        mock_k8s["get_resource"].return_value = make_incident_body(retry_count=2, max_retries=2)
        await _handle_failure("inc-1", NS)

        status = mock_k8s["patch_status"].call_args.args[2]
        assert status["phase"] == IncidentPhase.ESCALATED

    async def test_max_retries_read_from_cr_status(self, mock_k8s) -> None:
        # CR says maxRetries=1; after 1 retry, escalate
        mock_k8s["get_resource"].return_value = make_incident_body(retry_count=1, max_retries=1)
        await _handle_failure("inc-1", NS)
        status = mock_k8s["patch_status"].call_args.args[2]
        assert status["phase"] == IncidentPhase.ESCALATED

    async def test_max_retries_falls_back_to_settings_when_absent(self, mock_k8s) -> None:
        # CR status has no maxRetries → falls back to settings.max_retries
        incident = make_incident_body(retry_count=0)
        del incident["status"]["maxRetries"]
        mock_k8s["get_resource"].return_value = incident
        await _handle_failure("inc-1", NS)

        status = mock_k8s["patch_status"].call_args.args[2]
        if settings.max_retries > 0:
            # retry_count=0 < max_retries → FAILED path
            assert status["phase"] == IncidentPhase.FAILED
        else:
            assert status["phase"] == IncidentPhase.ESCALATED
