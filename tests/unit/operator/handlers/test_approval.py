"""Unit tests for the ApprovalRequest handler (decision dispatch + timeout)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from kubortex.operator.handlers.approval import check_approval_timeout, on_approval_decision
from kubortex.shared.types import ActionExecutionPhase, ApprovalRequestPhase, IncidentPhase

from ..conftest import make_approval_request_body

NS = "kubortex-system"


# ---------------------------------------------------------------------------
# on_approval_decision
# ---------------------------------------------------------------------------


class TestOnApprovalDecision:
    async def test_no_new_decision_skips(self, mock_k8s) -> None:
        body = make_approval_request_body()
        await on_approval_decision(body=body, name="ar-1", namespace=NS, new=None, old=None)
        mock_k8s["patch_status"].assert_not_awaited()

    async def test_old_already_set_skips(self, mock_k8s) -> None:
        body = make_approval_request_body()
        await on_approval_decision(
            body=body, name="ar-1", namespace=NS, new="approved", old="approved"
        )
        mock_k8s["patch_status"].assert_not_awaited()

    async def test_approved_patches_ar_creates_ae_and_sets_executing(self, mock_k8s) -> None:
        body = make_approval_request_body(incident_ref="inc-1")
        body["status"]["decidedBy"] = "alice"
        body["status"]["decidedAt"] = datetime.now(UTC).isoformat()

        await on_approval_decision(body=body, name="ar-1", namespace=NS, new="approved", old=None)

        # AR patched APPROVED
        calls = mock_k8s["patch_status"].call_args_list
        ar_call = next(c for c in calls if c.args[0] == "approvalrequests")
        assert ar_call.args[2]["phase"] == ApprovalRequestPhase.APPROVED

        # ActionExecution created
        mock_k8s["create_resource"].assert_awaited_once()
        ae_body = mock_k8s["create_resource"].call_args.args[1]
        assert ae_body["kind"] == "ActionExecution"
        assert ae_body["spec"]["approvalRequestRef"] == "ar-1"
        assert ae_body["spec"]["incidentRef"] == "inc-1"

        # Incident set to EXECUTING
        incident_call = next(c for c in calls if c.args[0] == "incidents")
        assert incident_call.args[2]["phase"] == IncidentPhase.EXECUTING

    async def test_approved_sets_initial_ae_phase_approved(self, mock_k8s) -> None:
        mock_k8s["get_resource"].return_value = {
            "status": {"autonomyProfile": "allow-profile"}
        }
        body = make_approval_request_body(incident_ref="inc-1")
        await on_approval_decision(body=body, name="ar-1", namespace=NS, new="approved", old=None)

        calls = mock_k8s["patch_status"].call_args_list
        ae_phase_call = next(
            c for c in calls if c.args[0] == "actionexecutions" and "phase" in c.args[2]
        )
        assert ae_phase_call.args[2]["phase"] == ActionExecutionPhase.APPROVED

    async def test_approved_increments_budget(self, mock_k8s) -> None:
        mock_k8s["get_resource"].return_value = {
            "status": {"autonomyProfile": "allow-profile"}
        }
        body = make_approval_request_body(incident_ref="inc-1")
        await on_approval_decision(body=body, name="ar-1", namespace=NS, new="approved", old=None)

        mock_k8s["update_usage"].assert_awaited_once()

    async def test_approved_ae_name_uses_incident_and_action_id(self, mock_k8s) -> None:
        body = make_approval_request_body(name="ar-1", incident_ref="inc-test")
        await on_approval_decision(body=body, name="ar-1", namespace=NS, new="approved", old=None)
        ae_body = mock_k8s["create_resource"].call_args.args[1]
        assert ae_body["metadata"]["name"] == "ae-inc-test-action-1"

    async def test_rejected_patches_ar_and_escalates_incident(self, mock_k8s) -> None:
        body = make_approval_request_body(incident_ref="inc-1")
        await on_approval_decision(body=body, name="ar-1", namespace=NS, new="rejected", old=None)

        mock_k8s["create_resource"].assert_not_awaited()
        calls = mock_k8s["patch_status"].call_args_list

        ar_call = next(c for c in calls if c.args[0] == "approvalrequests")
        assert ar_call.args[2]["phase"] == ApprovalRequestPhase.REJECTED

        incident_call = next(c for c in calls if c.args[0] == "incidents")
        assert incident_call.args[2]["phase"] == IncidentPhase.ESCALATED


# ---------------------------------------------------------------------------
# check_approval_timeout
# ---------------------------------------------------------------------------


class TestCheckApprovalTimeout:
    async def test_non_pending_phase_skips(self, mock_k8s) -> None:
        body = make_approval_request_body(phase="Approved")
        await check_approval_timeout(body=body, name="ar-1", namespace=NS)
        mock_k8s["patch_status"].assert_not_awaited()

    async def test_no_creation_timestamp_skips(self, mock_k8s) -> None:
        body = make_approval_request_body(phase="Pending")  # no creationTimestamp
        await check_approval_timeout(body=body, name="ar-1", namespace=NS)
        mock_k8s["patch_status"].assert_not_awaited()

    async def test_not_timed_out_skips(self, mock_k8s) -> None:
        recent = (datetime.now(UTC) - timedelta(minutes=5)).isoformat()
        body = make_approval_request_body(
            phase="Pending", timeout_minutes=30, creation_timestamp=recent
        )
        await check_approval_timeout(body=body, name="ar-1", namespace=NS)
        mock_k8s["patch_status"].assert_not_awaited()

    async def test_timed_out_patches_ar_timed_out(self, mock_k8s) -> None:
        old_ts = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
        body = make_approval_request_body(
            phase="Pending", timeout_minutes=30, creation_timestamp=old_ts, incident_ref="inc-1"
        )
        await check_approval_timeout(body=body, name="ar-1", namespace=NS)

        calls = mock_k8s["patch_status"].call_args_list
        ar_call = next(c for c in calls if c.args[0] == "approvalrequests")
        assert ar_call.args[2]["phase"] == ApprovalRequestPhase.TIMED_OUT

    async def test_timed_out_with_incident_ref_escalates_incident(self, mock_k8s) -> None:
        old_ts = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
        body = make_approval_request_body(
            phase="Pending", timeout_minutes=30, creation_timestamp=old_ts, incident_ref="inc-1"
        )
        await check_approval_timeout(body=body, name="ar-1", namespace=NS)

        calls = mock_k8s["patch_status"].call_args_list
        incident_call = next(c for c in calls if c.args[0] == "incidents")
        assert incident_call.args[2]["phase"] == IncidentPhase.ESCALATED

    async def test_timed_out_without_incident_ref_no_incident_patch(self, mock_k8s) -> None:
        old_ts = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
        body = make_approval_request_body(
            phase="Pending", timeout_minutes=30, creation_timestamp=old_ts, incident_ref=""
        )
        body["spec"]["incidentRef"] = ""
        await check_approval_timeout(body=body, name="ar-1", namespace=NS)

        calls = mock_k8s["patch_status"].call_args_list
        incident_calls = [c for c in calls if c.args[0] == "incidents"]
        assert len(incident_calls) == 0
