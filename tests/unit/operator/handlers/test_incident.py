"""Unit tests for the Incident handler (lifecycle governance)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any


from kubortex.operator.handlers.incident import (
    _build_investigation,
    _match_autonomy_profile,
    _scope_matches,
    _scope_specificity,
    _transition,
    check_escalation_deadline,
    on_incident_create,
    on_incident_failed,
)
from kubortex.operator.settings import GROUP, VERSION, settings
from kubortex.shared.models import IncidentSpec
from kubortex.shared.models.autonomy import AutonomyScope
from kubortex.shared.types import IncidentPhase

from ..conftest import make_autonomy_profile_resource, make_incident_body


# ---------------------------------------------------------------------------
# _build_investigation (pure helper)
# ---------------------------------------------------------------------------


class TestBuildInvestigation:
    def _spec(self, **kwargs) -> IncidentSpec:
        base: dict[str, Any] = {
            "severity": "high",
            "category": "error-rate",
            "summary": "Test",
            "signals": [],
        }
        base.update(kwargs)
        return IncidentSpec.model_validate(base)

    def test_kind_and_api_version(self) -> None:
        body = _build_investigation("inv-1", "inc-1", "ns", self._spec(), uid="test-uid")
        assert body["kind"] == "Investigation"
        assert body["apiVersion"] == f"{GROUP}/{VERSION}"

    def test_owner_reference_points_to_incident(self) -> None:
        body = _build_investigation("inv-1", "inc-1", "ns", self._spec(), uid="test-uid")
        owner = body["metadata"]["ownerReferences"][0]
        assert owner["name"] == "inc-1"
        assert owner["kind"] == "Incident"
        assert owner["controller"] is True

    def test_labels_include_incident_and_category(self) -> None:
        spec = self._spec()
        body = _build_investigation("inv-1", "inc-1", "ns", spec, uid="test-uid")
        labels = body["metadata"]["labels"]
        assert labels["kubortex.io/incident"] == "inc-1"
        assert labels["kubortex.io/category"] == spec.category

    def test_spec_fields_from_incident(self) -> None:
        spec = self._spec()
        body = _build_investigation("inv-1", "inc-1", "ns", spec, uid="test-uid")
        s = body["spec"]
        assert s["incidentRef"] == "inc-1"
        assert s["severity"] == spec.severity
        assert s["category"] == spec.category
        assert s["summary"] == spec.summary

    def test_max_iterations_and_timeout_from_settings(self) -> None:
        body = _build_investigation("inv-1", "inc-1", "ns", self._spec(), uid="test-uid")
        assert body["spec"]["maxIterations"] == settings.investigation_max_iterations
        assert body["spec"]["deadlineSeconds"] == settings.investigation_timeout_seconds

    def test_target_ref_included_when_present(self) -> None:
        spec = self._spec(targetRef={"kind": "Deployment", "namespace": "ns", "name": "app"})
        body = _build_investigation("inv-1", "inc-1", "ns", spec, uid="test-uid")
        assert body["spec"]["targetRef"] is not None
        assert body["spec"]["targetRef"]["name"] == "app"

    def test_target_ref_none_when_absent(self) -> None:
        body = _build_investigation("inv-1", "inc-1", "ns", self._spec(), uid="test-uid")
        assert body["spec"]["targetRef"] is None

    def test_signals_serialized_with_alias(self) -> None:
        spec = self._spec(
            signals=[
                {
                    "alertname": "Alert1",
                    "severity": "high",
                    "summary": "s",
                    "observedAt": "2026-01-01T00:00:00Z",
                }
            ]
        )
        body = _build_investigation("inv-1", "inc-1", "ns", spec, uid="test-uid")
        sig = body["spec"]["signals"][0]
        assert "observedAt" in sig  # alias used, not snake_case


# ---------------------------------------------------------------------------
# _scope_matches (pure helper)
# ---------------------------------------------------------------------------


class TestScopeMatches:
    def _spec(
        self,
        severity: str = "high",
        category: str = "error-rate",
        target_ns: str | None = None,
    ) -> IncidentSpec:
        data: dict[str, Any] = {"severity": severity, "category": category, "summary": "t"}
        if target_ns is not None:
            data["targetRef"] = {"kind": "Deployment", "namespace": target_ns, "name": "app"}
        return IncidentSpec.model_validate(data)

    def _scope(self, **kwargs) -> AutonomyScope:
        return AutonomyScope.model_validate(kwargs)

    # --- catch-all (empty scope) ---

    def test_empty_scope_matches_anything(self) -> None:
        assert _scope_matches(self._scope(), self._spec(), None) is True

    def test_empty_scope_matches_incident_with_no_target_ref(self) -> None:
        assert _scope_matches(self._scope(), self._spec(target_ns=None), None) is True

    # --- severity filter ---

    def test_severity_in_list_matches(self) -> None:
        scope = self._scope(severities=["high", "critical"])
        assert _scope_matches(scope, self._spec(severity="high"), None) is True

    def test_severity_not_in_list_rejects(self) -> None:
        scope = self._scope(severities=["critical"])
        assert _scope_matches(scope, self._spec(severity="warning"), None) is False

    def test_empty_severities_matches_any_severity(self) -> None:
        scope = self._scope(severities=[])
        assert _scope_matches(scope, self._spec(severity="info"), None) is True

    # --- category filter ---

    def test_category_in_list_matches(self) -> None:
        scope = self._scope(categories=["error-rate", "latency"])
        assert _scope_matches(scope, self._spec(category="error-rate"), None) is True

    def test_category_not_in_list_rejects(self) -> None:
        scope = self._scope(categories=["latency"])
        assert _scope_matches(scope, self._spec(category="error-rate"), None) is False

    # --- matchNames ---

    def test_target_ns_in_match_names_matches(self) -> None:
        scope = self._scope(namespaces={"matchNames": ["payments", "orders"]})
        assert _scope_matches(scope, self._spec(target_ns="payments"), {}) is True

    def test_target_ns_not_in_match_names_rejects(self) -> None:
        scope = self._scope(namespaces={"matchNames": ["payments"]})
        assert _scope_matches(scope, self._spec(target_ns="staging"), {}) is False

    def test_match_names_without_target_ref_rejects(self) -> None:
        scope = self._scope(namespaces={"matchNames": ["payments"]})
        assert _scope_matches(scope, self._spec(target_ns=None), None) is False

    # --- matchLabels ---

    def test_match_labels_subset_of_ns_labels_matches(self) -> None:
        scope = self._scope(namespaces={"matchLabels": {"team": "sre"}})
        assert _scope_matches(scope, self._spec(target_ns="ns"), {"team": "sre", "env": "prod"}) is True

    def test_match_labels_not_subset_rejects(self) -> None:
        scope = self._scope(namespaces={"matchLabels": {"team": "sre"}})
        assert _scope_matches(scope, self._spec(target_ns="ns"), {"team": "platform"}) is False

    def test_match_labels_without_target_ref_rejects(self) -> None:
        scope = self._scope(namespaces={"matchLabels": {"team": "sre"}})
        assert _scope_matches(scope, self._spec(target_ns=None), None) is False

    def test_match_labels_with_no_ns_labels_rejects(self) -> None:
        # targetRef present but ns_labels is None (fetch failed)
        scope = self._scope(namespaces={"matchLabels": {"team": "sre"}})
        assert _scope_matches(scope, self._spec(target_ns="ns"), None) is False

    # --- combined ---

    def test_all_constraints_satisfied_matches(self) -> None:
        scope = self._scope(
            severities=["high"],
            categories=["error-rate"],
            namespaces={"matchNames": ["payments"], "matchLabels": {"env": "prod"}},
        )
        spec = self._spec(severity="high", category="error-rate", target_ns="payments")
        assert _scope_matches(scope, spec, {"env": "prod", "team": "sre"}) is True

    def test_one_constraint_fails_rejects(self) -> None:
        scope = self._scope(
            severities=["high"],
            categories=["latency"],  # incident is error-rate
        )
        assert _scope_matches(scope, self._spec(severity="high", category="error-rate"), None) is False


# ---------------------------------------------------------------------------
# _scope_specificity (pure helper)
# ---------------------------------------------------------------------------


class TestScopeSpecificity:
    def _scope(self, **kwargs) -> AutonomyScope:
        return AutonomyScope.model_validate(kwargs)

    def test_empty_scope_scores_zero(self) -> None:
        assert _scope_specificity(self._scope()) == 0

    def test_each_severity_adds_one(self) -> None:
        assert _scope_specificity(self._scope(severities=["high", "critical"])) == 2

    def test_each_category_adds_one(self) -> None:
        assert _scope_specificity(self._scope(categories=["error-rate"])) == 1

    def test_each_match_name_adds_one(self) -> None:
        assert _scope_specificity(self._scope(namespaces={"matchNames": ["a", "b"]})) == 2

    def test_each_match_label_adds_one(self) -> None:
        assert _scope_specificity(self._scope(namespaces={"matchLabels": {"k": "v"}})) == 1

    def test_combined_score(self) -> None:
        scope = self._scope(
            severities=["high"],
            categories=["error-rate", "latency"],
            namespaces={"matchNames": ["payments"], "matchLabels": {"env": "prod", "team": "sre"}},
        )
        assert _scope_specificity(scope) == 6  # 1 + 2 + 1 + 2


# ---------------------------------------------------------------------------
# _match_autonomy_profile
# ---------------------------------------------------------------------------


class TestMatchAutonomyProfile:
    def _spec(
        self,
        severity: str = "high",
        category: str = "error-rate",
        target_ns: str | None = None,
    ) -> IncidentSpec:
        data: dict[str, Any] = {"severity": severity, "category": category, "summary": "test"}
        if target_ns is not None:
            data["targetRef"] = {"kind": "Deployment", "namespace": target_ns, "name": "app"}
        return IncidentSpec.model_validate(data)

    async def test_no_profiles_returns_none(self, mock_k8s) -> None:
        mock_k8s["list_resources"].return_value = []
        assert await _match_autonomy_profile(self._spec(), "ns") is None

    async def test_catch_all_profile_matches_any_incident(self, mock_k8s) -> None:
        mock_k8s["list_resources"].return_value = [make_autonomy_profile_resource("catch-all")]
        assert await _match_autonomy_profile(self._spec(), "ns") == "catch-all"

    async def test_severity_mismatch_returns_none(self, mock_k8s) -> None:
        mock_k8s["list_resources"].return_value = [
            make_autonomy_profile_resource("p", severities=["critical"])
        ]
        assert await _match_autonomy_profile(self._spec(severity="warning"), "ns") is None

    async def test_category_mismatch_returns_none(self, mock_k8s) -> None:
        mock_k8s["list_resources"].return_value = [
            make_autonomy_profile_resource("p", categories=["latency"])
        ]
        assert await _match_autonomy_profile(self._spec(category="error-rate"), "ns") is None

    async def test_match_names_hit_returns_profile(self, mock_k8s) -> None:
        mock_k8s["list_resources"].return_value = [
            make_autonomy_profile_resource("p", match_names=["payments"])
        ]
        assert await _match_autonomy_profile(self._spec(target_ns="payments"), "ns") == "p"

    async def test_match_names_miss_returns_none(self, mock_k8s) -> None:
        mock_k8s["list_resources"].return_value = [
            make_autonomy_profile_resource("p", match_names=["payments"])
        ]
        assert await _match_autonomy_profile(self._spec(target_ns="staging"), "ns") is None

    async def test_match_names_without_target_ref_returns_none(self, mock_k8s) -> None:
        mock_k8s["list_resources"].return_value = [
            make_autonomy_profile_resource("p", match_names=["payments"])
        ]
        assert await _match_autonomy_profile(self._spec(target_ns=None), "ns") is None

    async def test_match_labels_fetches_namespace_labels(self, mock_k8s) -> None:
        mock_k8s["get_namespace_labels"].return_value = {"env": "prod"}
        mock_k8s["list_resources"].return_value = [
            make_autonomy_profile_resource("p", match_labels={"env": "prod"})
        ]
        assert await _match_autonomy_profile(self._spec(target_ns="payments"), "ns") == "p"
        mock_k8s["get_namespace_labels"].assert_awaited_once_with("payments")

    async def test_match_labels_mismatch_returns_none(self, mock_k8s) -> None:
        mock_k8s["get_namespace_labels"].return_value = {"env": "staging"}
        mock_k8s["list_resources"].return_value = [
            make_autonomy_profile_resource("p", match_labels={"env": "prod"})
        ]
        assert await _match_autonomy_profile(self._spec(target_ns="ns"), "ns") is None

    async def test_match_labels_without_target_ref_returns_none(self, mock_k8s) -> None:
        mock_k8s["list_resources"].return_value = [
            make_autonomy_profile_resource("p", match_labels={"env": "prod"})
        ]
        assert await _match_autonomy_profile(self._spec(target_ns=None), "ns") is None
        mock_k8s["get_namespace_labels"].assert_not_awaited()

    async def test_more_specific_profile_wins(self, mock_k8s) -> None:
        # catch-all (score 0) vs severity-scoped (score 1)
        mock_k8s["list_resources"].return_value = [
            make_autonomy_profile_resource("catch-all"),
            make_autonomy_profile_resource("specific", severities=["high"]),
        ]
        assert await _match_autonomy_profile(self._spec(severity="high"), "ns") == "specific"

    async def test_alphabetical_tiebreak_on_equal_specificity(self, mock_k8s) -> None:
        mock_k8s["list_resources"].return_value = [
            make_autonomy_profile_resource("zebra", severities=["high"]),
            make_autonomy_profile_resource("alpha", severities=["high"]),
        ]
        assert await _match_autonomy_profile(self._spec(severity="high"), "ns") == "alpha"

    async def test_no_target_ref_skips_ns_label_fetch(self, mock_k8s) -> None:
        mock_k8s["list_resources"].return_value = [make_autonomy_profile_resource("p")]
        await _match_autonomy_profile(self._spec(target_ns=None), "ns")
        mock_k8s["get_namespace_labels"].assert_not_awaited()


# ---------------------------------------------------------------------------
# _transition
# ---------------------------------------------------------------------------


class TestTransition:
    async def test_patches_status_with_phase_and_timeline(self, mock_k8s) -> None:
        await _transition("inc-1", "ns", IncidentPhase.ESCALATED, "Test reason")
        mock_k8s["patch_status"].assert_awaited_once()
        args = mock_k8s["patch_status"].call_args.args
        status = args[2]
        assert status["phase"] == IncidentPhase.ESCALATED
        assert status["timeline"][0]["detail"] == "Test reason"
        assert status["timeline"][0]["event"] == "PhaseTransition"


# ---------------------------------------------------------------------------
# on_incident_create
# ---------------------------------------------------------------------------


class TestOnIncidentCreate:
    async def test_no_matching_profile_escalates_incident(self, mock_k8s) -> None:
        mock_k8s["list_resources"].return_value = []
        body = make_incident_body()
        await on_incident_create(body=body, name="inc-1", namespace="ns")

        mock_k8s["create_resource"].assert_not_awaited()
        patch_call = mock_k8s["patch_status"].call_args.args
        assert patch_call[2]["phase"] == IncidentPhase.ESCALATED

    async def test_creates_investigation_cr_on_match(self, mock_k8s) -> None:
        mock_k8s["list_resources"].return_value = [make_autonomy_profile_resource("sre")]
        mock_k8s["get_resource"].return_value = make_autonomy_profile_resource("sre")
        body = make_incident_body()

        await on_incident_create(body=body, name="inc-1", namespace="ns")

        mock_k8s["create_resource"].assert_awaited_once()
        plural, inv_body = mock_k8s["create_resource"].call_args.args[:2]
        assert plural == "investigations"
        assert inv_body["kind"] == "Investigation"
        assert inv_body["metadata"]["name"] == "inv-inc-1"

    async def test_status_patched_with_investigating_phase(self, mock_k8s) -> None:
        mock_k8s["list_resources"].return_value = [make_autonomy_profile_resource("sre")]
        mock_k8s["get_resource"].return_value = make_autonomy_profile_resource("sre")
        body = make_incident_body()

        await on_incident_create(body=body, name="inc-1", namespace="ns")

        # patch_status is called twice: once by _transition (no profile) path is skipped here
        # Here it's called once at the end of on_incident_create
        last_call = mock_k8s["patch_status"].call_args.args
        status = last_call[2]
        assert status["phase"] == IncidentPhase.INVESTIGATING
        assert status["autonomyProfile"] == "sre"
        assert status["investigationRef"] == "inv-inc-1"
        assert "escalationDeadline" in status

    async def test_escalation_deadline_from_profile(self, mock_k8s) -> None:
        profile = make_autonomy_profile_resource("sre", deadline_minutes=20)
        mock_k8s["list_resources"].return_value = [profile]
        mock_k8s["get_resource"].return_value = profile
        body = make_incident_body()

        before = datetime.now(UTC)
        await on_incident_create(body=body, name="inc-1", namespace="ns")
        after = datetime.now(UTC)

        status = mock_k8s["patch_status"].call_args.args[2]
        deadline = datetime.fromisoformat(status["escalationDeadline"])
        assert before + timedelta(minutes=20) <= deadline <= after + timedelta(minutes=20)

    async def test_escalation_deadline_falls_back_to_settings(self, mock_k8s) -> None:
        # Profile spec with no escalationDeadlineMinutes field
        profile = {"metadata": {"name": "sre"}, "spec": {}, "status": {}}
        mock_k8s["list_resources"].return_value = [profile]
        mock_k8s["get_resource"].return_value = profile
        body = make_incident_body()

        before = datetime.now(UTC)
        await on_incident_create(body=body, name="inc-1", namespace="ns")
        after = datetime.now(UTC)

        status = mock_k8s["patch_status"].call_args.args[2]
        deadline = datetime.fromisoformat(status["escalationDeadline"])
        expected_minutes = settings.escalation_deadline_minutes
        assert before + timedelta(minutes=expected_minutes) <= deadline
        assert deadline <= after + timedelta(minutes=expected_minutes)


# ---------------------------------------------------------------------------
# check_escalation_deadline
# ---------------------------------------------------------------------------


class TestCheckEscalationDeadline:
    async def test_non_investigating_phase_no_patch(self, mock_k8s) -> None:
        body = make_incident_body(phase="Resolved")
        await check_escalation_deadline(body=body, name="inc", namespace="ns")
        mock_k8s["patch_status"].assert_not_awaited()

    async def test_no_deadline_no_patch(self, mock_k8s) -> None:
        body = make_incident_body(phase="Investigating")
        await check_escalation_deadline(body=body, name="inc", namespace="ns")
        mock_k8s["patch_status"].assert_not_awaited()

    async def test_deadline_in_future_no_patch(self, mock_k8s) -> None:
        future = (datetime.now(UTC) + timedelta(minutes=10)).isoformat()
        body = make_incident_body(phase="Investigating", deadline=future)
        await check_escalation_deadline(body=body, name="inc", namespace="ns")
        mock_k8s["patch_status"].assert_not_awaited()

    async def test_deadline_exceeded_escalates(self, mock_k8s) -> None:
        past = (datetime.now(UTC) - timedelta(minutes=5)).isoformat()
        body = make_incident_body(phase="Investigating", deadline=past)
        await check_escalation_deadline(body=body, name="inc", namespace="ns")
        mock_k8s["patch_status"].assert_awaited_once()
        status = mock_k8s["patch_status"].call_args.args[2]
        assert status["phase"] == IncidentPhase.ESCALATED


# ---------------------------------------------------------------------------
# on_incident_failed
# ---------------------------------------------------------------------------


class TestOnIncidentFailed:
    async def test_non_failed_phase_skips(self, mock_k8s) -> None:
        body = make_incident_body(phase="Investigating", profile="allow-profile")
        await on_incident_failed(body=body, name="inc", namespace="ns", new="Investigating")
        mock_k8s["create_resource"].assert_not_awaited()
        mock_k8s["patch_status"].assert_not_awaited()

    async def test_no_profile_escalates(self, mock_k8s) -> None:
        body = make_incident_body(phase="Failed")
        await on_incident_failed(body=body, name="inc", namespace="ns", new="Failed")
        calls = mock_k8s["patch_status"].call_args_list
        incident_call = next(c for c in calls if c.args[0] == "incidents")
        assert incident_call.args[2]["phase"] == IncidentPhase.ESCALATED

    async def test_missing_profile_escalates(self, mock_k8s) -> None:
        from kubernetes_asyncio.client import ApiException

        mock_k8s["get_resource"].side_effect = ApiException(status=404)
        body = make_incident_body(phase="Failed", profile="gone-profile")
        await on_incident_failed(body=body, name="inc", namespace="ns", new="Failed")
        calls = mock_k8s["patch_status"].call_args_list
        incident_call = next(c for c in calls if c.args[0] == "incidents")
        assert incident_call.args[2]["phase"] == IncidentPhase.ESCALATED

    async def test_creates_new_investigation_with_retry_suffix(self, mock_k8s) -> None:
        profile = make_autonomy_profile_resource(name="allow-profile")
        mock_k8s["get_resource"].return_value = profile
        body = make_incident_body(
            name="inc-retry",
            phase="Failed",
            profile="allow-profile",
            retry_count=1,
            severity="high",
            category="error-rate",
        )
        body["metadata"]["uid"] = "test-uid"
        await on_incident_failed(body=body, name="inc-retry", namespace="ns", new="Failed")

        mock_k8s["create_resource"].assert_awaited_once()
        created_plural, created_body = mock_k8s["create_resource"].call_args.args
        assert created_plural == "investigations"
        assert created_body["metadata"]["name"] == "inv-inc-retry-r1"

    async def test_patches_incident_back_to_investigating(self, mock_k8s) -> None:
        profile = make_autonomy_profile_resource(name="allow-profile")
        mock_k8s["get_resource"].return_value = profile
        body = make_incident_body(
            name="inc-retry",
            phase="Failed",
            profile="allow-profile",
            retry_count=2,
        )
        body["metadata"]["uid"] = "test-uid"
        await on_incident_failed(body=body, name="inc-retry", namespace="ns", new="Failed")

        calls = mock_k8s["patch_status"].call_args_list
        # Should patch investigation to Pending and incident to Investigating
        inv_call = next(c for c in calls if c.args[0] == "investigations")
        assert inv_call.args[2]["phase"] == "Pending"
        inc_call = next(c for c in calls if c.args[0] == "incidents")
        assert inc_call.args[2]["phase"] == IncidentPhase.INVESTIGATING
        assert inc_call.args[2]["investigationRef"] == "inv-inc-retry-r2"
