"""Unit tests for the Investigation handler (phase governance)."""

from __future__ import annotations

from kubortex.operator.handlers.investigation import (
    on_investigation_claimed,
    on_investigation_phase_terminal,
    on_investigation_result,
)
from kubortex.shared.types import IncidentPhase, InvestigationPhase

from ..conftest import make_investigation_body

NS = "kubortex-system"


# ---------------------------------------------------------------------------
# on_investigation_claimed
# ---------------------------------------------------------------------------


class TestOnInvestigationClaimed:
    async def test_new_is_none_skips(self, mock_k8s) -> None:
        body = make_investigation_body(phase="Pending")
        await on_investigation_claimed(body=body, name="inv", namespace=NS, new=None, old=None)
        mock_k8s["patch_status"].assert_not_awaited()

    async def test_old_already_set_skips(self, mock_k8s) -> None:
        body = make_investigation_body(phase="Pending")
        await on_investigation_claimed(
            body=body, name="inv", namespace=NS, new="worker-1", old="worker-0"
        )
        mock_k8s["patch_status"].assert_not_awaited()

    async def test_non_pending_phase_skips(self, mock_k8s) -> None:
        body = make_investigation_body(phase="InProgress")
        await on_investigation_claimed(
            body=body, name="inv", namespace=NS, new="worker-1", old=None
        )
        mock_k8s["patch_status"].assert_not_awaited()

    async def test_valid_claim_patches_in_progress(self, mock_k8s) -> None:
        body = make_investigation_body(phase="Pending")
        await on_investigation_claimed(
            body=body, name="inv-1", namespace=NS, new="worker-1", old=None
        )
        mock_k8s["patch_status"].assert_awaited_once()
        plural, name, status_patch = mock_k8s["patch_status"].call_args.args
        assert plural == "investigations"
        assert name == "inv-1"
        assert status_patch["phase"] == InvestigationPhase.IN_PROGRESS


# ---------------------------------------------------------------------------
# on_investigation_result
# ---------------------------------------------------------------------------


class TestOnInvestigationResult:
    async def test_new_is_none_skips(self, mock_k8s) -> None:
        body = make_investigation_body(phase="InProgress")
        await on_investigation_result(body=body, name="inv", namespace=NS, new=None)
        mock_k8s["patch_status"].assert_not_awaited()

    async def test_non_in_progress_phase_skips(self, mock_k8s) -> None:
        body = make_investigation_body(phase="Pending")
        await on_investigation_result(body=body, name="inv", namespace=NS, new={"hypothesis": "x"})
        mock_k8s["patch_status"].assert_not_awaited()

    async def test_valid_result_patches_investigation_completed(self, mock_k8s) -> None:
        body = make_investigation_body(phase="InProgress")
        await on_investigation_result(
            body=body, name="inv-1", namespace=NS, new={"hypothesis": "OOM"}
        )
        # At minimum, Investigation is patched COMPLETED
        calls = mock_k8s["patch_status"].call_args_list
        inv_call = next(c for c in calls if c.args[0] == "investigations")
        assert inv_call.args[2]["phase"] == InvestigationPhase.COMPLETED

    async def test_no_incident_ref_does_not_patch_incident(self, mock_k8s) -> None:
        body = make_investigation_body(phase="InProgress", incident_ref="")
        await on_investigation_result(
            body=body, name="inv-1", namespace=NS, new={"hypothesis": "h"}
        )
        calls = mock_k8s["patch_status"].call_args_list
        incident_calls = [c for c in calls if c.args[0] == "incidents"]
        assert len(incident_calls) == 0

    async def test_with_incident_ref_patches_incident_synopsis(self, mock_k8s) -> None:
        result = {
            "hypothesis": "Memory leak",
            "confidence": 0.91,
            "evidence": [{"skill": "prom"}, {"skill": "logs"}],
            "recommendedActions": [{"type": "restart-pod"}],
        }
        body = make_investigation_body(phase="InProgress", incident_ref="inc-1")
        await on_investigation_result(body=body, name="inv-1", namespace=NS, new=result)

        calls = mock_k8s["patch_status"].call_args_list
        incident_call = next(c for c in calls if c.args[0] == "incidents")
        synopsis = incident_call.args[2]["investigation"]
        assert synopsis["hypothesis"] == "Memory leak"
        assert synopsis["confidence"] == 0.91
        assert synopsis["evidenceCount"] == 2
        assert synopsis["proposedActionCount"] == 1

    async def test_non_dict_result_skips_incident_patch(self, mock_k8s) -> None:
        body = make_investigation_body(phase="InProgress", incident_ref="inc-1")
        await on_investigation_result(body=body, name="inv-1", namespace=NS, new="completed")
        calls = mock_k8s["patch_status"].call_args_list
        incident_calls = [c for c in calls if c.args[0] == "incidents"]
        assert len(incident_calls) == 0

    async def test_result_without_escalate_transitions_incident_to_remediation_planned(
        self, mock_k8s
    ) -> None:
        result = {"hypothesis": "OOM", "confidence": 0.8, "evidence": [], "recommendedActions": []}
        body = make_investigation_body(phase="InProgress", incident_ref="inc-1")
        await on_investigation_result(body=body, name="inv-1", namespace=NS, new=result)

        calls = mock_k8s["patch_status"].call_args_list
        phase_calls = [c for c in calls if c.args[0] == "incidents" and "phase" in c.args[2]]
        assert len(phase_calls) == 1
        assert phase_calls[0].args[2]["phase"] == IncidentPhase.REMEDIATION_PLANNED

    async def test_result_with_escalate_true_transitions_incident_to_escalated(
        self, mock_k8s
    ) -> None:
        result = {"hypothesis": "Unknown", "confidence": 0.1, "escalate": True}
        body = make_investigation_body(phase="InProgress", incident_ref="inc-1")
        await on_investigation_result(body=body, name="inv-1", namespace=NS, new=result)

        calls = mock_k8s["patch_status"].call_args_list
        phase_calls = [c for c in calls if c.args[0] == "incidents" and "phase" in c.args[2]]
        assert len(phase_calls) == 1
        assert phase_calls[0].args[2]["phase"] == IncidentPhase.ESCALATED


# ---------------------------------------------------------------------------
# on_investigation_phase_terminal
# ---------------------------------------------------------------------------


class TestOnInvestigationPhaseTerminal:
    async def test_non_terminal_phase_skips(self, mock_k8s) -> None:
        body = make_investigation_body(phase="InProgress", incident_ref="inc-1")
        await on_investigation_phase_terminal(
            body=body, name="inv-1", namespace=NS, new="InProgress"
        )
        mock_k8s["patch_status"].assert_not_awaited()

    async def test_timed_out_escalates_incident(self, mock_k8s) -> None:
        body = make_investigation_body(phase="TimedOut", incident_ref="inc-1")
        await on_investigation_phase_terminal(
            body=body, name="inv-1", namespace=NS, new="TimedOut"
        )
        calls = mock_k8s["patch_status"].call_args_list
        incident_call = next(c for c in calls if c.args[0] == "incidents")
        assert incident_call.args[2]["phase"] == IncidentPhase.ESCALATED

    async def test_cancelled_escalates_incident(self, mock_k8s) -> None:
        body = make_investigation_body(phase="Cancelled", incident_ref="inc-1")
        await on_investigation_phase_terminal(
            body=body, name="inv-1", namespace=NS, new="Cancelled"
        )
        calls = mock_k8s["patch_status"].call_args_list
        incident_call = next(c for c in calls if c.args[0] == "incidents")
        assert incident_call.args[2]["phase"] == IncidentPhase.ESCALATED

    async def test_terminal_without_incident_ref_skips(self, mock_k8s) -> None:
        body = make_investigation_body(phase="TimedOut", incident_ref="")
        await on_investigation_phase_terminal(
            body=body, name="inv-1", namespace=NS, new="TimedOut"
        )
        mock_k8s["patch_status"].assert_not_awaited()
