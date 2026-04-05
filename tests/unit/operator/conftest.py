"""Shared fixtures and factories for kubortex.operator unit tests."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from kubortex.operator.policy import ActionContext
from kubortex.shared.models.autonomy import BudgetUsage
from kubortex.shared.types import Severity


# ---------------------------------------------------------------------------
# Domain object factories
# ---------------------------------------------------------------------------


def make_autonomy_profile_resource(
    name: str = "default",
    severities: list[str] | None = None,
    categories: list[str] | None = None,
    match_names: list[str] | None = None,
    match_labels: dict[str, str] | None = None,
    rules: list[dict] | None = None,
    deadline_minutes: int = 15,
    namespace: str = "kubortex-system",
) -> dict:
    """Build a minimal AutonomyProfile CRD dict."""
    spec: dict = {"escalationDeadlineMinutes": deadline_minutes}
    scope: dict = {}
    if severities is not None:
        scope["severities"] = severities
    if categories is not None:
        scope["categories"] = categories
    if match_names is not None or match_labels is not None:
        ns_sel: dict = {}
        if match_names is not None:
            ns_sel["matchNames"] = match_names
        if match_labels is not None:
            ns_sel["matchLabels"] = match_labels
        scope["namespaces"] = ns_sel
    if scope:
        spec["scope"] = scope
    if rules is not None:
        spec["autonomyRules"] = rules
    return {
        "metadata": {"name": name, "namespace": namespace},
        "spec": spec,
        "status": {"budgetUsage": {}},
    }


def make_budget_usage(
    pods_killed: int = 0,
    rollbacks: int = 0,
    scale_ups: int = 0,
    active: int = 0,
) -> BudgetUsage:
    """Build a BudgetUsage model instance."""
    return BudgetUsage(
        podsKilledThisHour=pods_killed,
        rollbacksToday=rollbacks,
        scaleUpsThisHour=scale_ups,
        activeRemediations=active,
    )


def make_action_context(
    action_type: str = "restart-pod",
    severity: Severity = Severity.HIGH,
    confidence: float = 0.9,
    target_key: str = "default/my-pod",
) -> ActionContext:
    """Build an ActionContext for policy tests."""
    return ActionContext(
        action_type=action_type,
        severity=severity,
        confidence=confidence,
        target_key=target_key,
    )


def make_incident_body(
    name: str = "inc-test",
    phase: str = "Detected",
    severity: str = "high",
    category: str = "error-rate",
    profile: str | None = None,
    deadline: str | None = None,
    retry_count: int = 0,
    max_retries: int = 2,
    namespace: str = "kubortex-system",
    target_ref: dict | None = None,
) -> dict:
    """Build a minimal Incident CRD dict."""
    status: dict = {"phase": phase, "retryCount": retry_count, "maxRetries": max_retries}
    if profile:
        status["autonomyProfile"] = profile
    if deadline:
        status["escalationDeadline"] = deadline
    spec: dict = {
        "severity": severity,
        "category": category,
        "summary": "Test incident",
        "source": "alertmanager",
        "signals": [],
    }
    if target_ref:
        spec["targetRef"] = target_ref
    return {
        "metadata": {"name": name, "namespace": namespace},
        "spec": spec,
        "status": status,
    }


def make_investigation_body(
    name: str = "inv-test",
    incident_ref: str = "inc-test",
    phase: str = "Pending",
    claimed_by: str | None = None,
    result: dict | None = None,
) -> dict:
    """Build a minimal Investigation CRD dict."""
    status: dict = {"phase": phase}
    if claimed_by:
        status["claimedBy"] = claimed_by
    if result is not None:
        status["result"] = result
    return {
        "metadata": {"name": name, "namespace": "kubortex-system"},
        "spec": {"incidentRef": incident_ref},
        "status": status,
    }


def make_action_proposal(
    action_id: str = "action-1",
    action_type: str = "restart-pod",
) -> dict:
    """Build a single action proposal dict (camelCase for CRD spec)."""
    return {
        "id": action_id,
        "type": action_type,
        "target": {"kind": "Pod", "namespace": "default", "name": "my-pod"},
        "parameters": {},
        "rationale": "Test rationale",
        "riskTier": "low",
        "expectedEffect": "Pod restarts cleanly",
        "reversible": True,
    }


def make_remediation_plan_body(
    name: str = "rp-test",
    incident_ref: str = "inc-test",
    actions: list[dict] | None = None,
    confidence: float = 0.9,
) -> dict:
    """Build a minimal RemediationPlan CRD dict."""
    return {
        "metadata": {"name": name, "namespace": "kubortex-system"},
        "spec": {
            "incidentRef": incident_ref,
            "investigationRef": f"inv-{incident_ref}",
            "hypothesis": "Test hypothesis",
            "confidence": confidence,
            "actions": actions if actions is not None else [make_action_proposal()],
        },
    }


def make_approval_request_body(
    name: str = "ar-test",
    incident_ref: str = "inc-test",
    investigation_ref: str = "inv-inc-test",
    phase: str = "Pending",
    decision: str | None = None,
    timeout_minutes: int = 30,
    creation_timestamp: str | None = None,
) -> dict:
    """Build a minimal ApprovalRequest CRD dict."""
    status: dict = {"phase": phase}
    if decision:
        status["decision"] = decision
        status["decidedBy"] = "alice@example.com"
        status["decidedAt"] = datetime.now(UTC).isoformat()
    metadata: dict = {"name": name, "namespace": "kubortex-system"}
    if creation_timestamp:
        metadata["creationTimestamp"] = creation_timestamp
    return {
        "metadata": metadata,
        "spec": {
            "incidentRef": incident_ref,
            "investigationRef": investigation_ref,
            "remediationPlanRef": "rp-test",
            "action": {
                "id": "action-1",
                "type": "restart-pod",
                "target": {"kind": "Pod", "namespace": "default", "name": "my-pod"},
                "parameters": {},
                "rationale": "test",
                "riskTier": "low",
            },
            "timeoutMinutes": timeout_minutes,
        },
        "status": status,
    }


def make_action_execution_body(
    name: str = "ae-test",
    incident_ref: str = "inc-test",
    phase: str = "Approved",
    error: str | None = None,
    rollback_triggered: bool = False,
    improved: bool | None = None,
) -> dict:
    """Build a minimal ActionExecution CRD dict."""
    status: dict = {"phase": phase}
    if error:
        status["error"] = error
    if rollback_triggered:
        status["rollback"] = {"triggered": True}
        status["verification"] = {"improved": False}
    elif improved is not None:
        status["verification"] = {"improved": improved}
    return {
        "metadata": {"name": name, "namespace": "kubortex-system"},
        "spec": {"incidentRef": incident_ref},
        "status": status,
    }


# ---------------------------------------------------------------------------
# Pytest fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_k8s(monkeypatch) -> dict:
    """Patch all kubortex.shared.k8s functions and budget helpers in operator handler modules.

    Top-level imports are patched on each handler module namespace.
    Lazy in-function imports are covered by patching kubortex.shared.k8s directly.
    Budget helpers (load_usage, persist_usage, increment_usage, decrement_active) are also mocked.
    """
    default_usage = BudgetUsage()

    mocks: dict = {
        # K8s helpers
        "create_resource": AsyncMock(return_value={}),
        "get_resource": AsyncMock(return_value={}),
        "list_resources": AsyncMock(return_value=[]),
        "patch_status": AsyncMock(return_value={}),
        "patch_spec": AsyncMock(return_value={}),
        # Namespace label lookup (returns empty labels by default)
        "get_namespace_labels": AsyncMock(return_value={}),
        # Budget helpers
        "load_usage": AsyncMock(return_value=default_usage),
        "persist_usage": AsyncMock(return_value=None),
        "increment_usage": MagicMock(return_value=default_usage),
        "decrement_active": MagicMock(return_value=default_usage),
        "update_usage": AsyncMock(return_value=default_usage),
    }

    k8s_fns = ["create_resource", "get_resource", "list_resources", "patch_status", "patch_spec"]
    budget_fns = ["load_usage", "persist_usage", "increment_usage", "decrement_active", "update_usage"]

    for handler in [
        "incident",
        "investigation",
        "remediation",
        "approval",
        "action",
        "autonomy",
    ]:
        for fn in k8s_fns:
            monkeypatch.setattr(
                f"kubortex.operator.handlers.{handler}.{fn}", mocks[fn], raising=False
            )
        for fn in budget_fns:
            monkeypatch.setattr(
                f"kubortex.operator.handlers.{handler}.{fn}", mocks[fn], raising=False
            )

    monkeypatch.setattr(
        "kubortex.operator.handlers.incident._get_namespace_labels",
        mocks["get_namespace_labels"],
    )

    for fn in k8s_fns:
        monkeypatch.setattr(f"kubortex.shared.crds.{fn}", mocks[fn])
    for fn in budget_fns:
        monkeypatch.setattr(f"kubortex.operator.budget.{fn}", mocks[fn], raising=False)

    return mocks
