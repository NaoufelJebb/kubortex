"""Prometheus metric definitions for all Kubortex components.

All metrics use the ``kubortex_`` prefix.  Import this module to register
the metrics; expose them via the Prometheus client HTTP handler.
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

# ---------------------------------------------------------------------------
# Incidents
# ---------------------------------------------------------------------------

INCIDENTS_CREATED = Counter(
    "kubortex_incidents_created_total",
    "Total incidents created",
    ["category", "severity"],
)

INCIDENTS_ACTIVE = Gauge(
    "kubortex_incidents_active",
    "Currently active incidents by phase",
    ["phase"],
)

# ---------------------------------------------------------------------------
# Investigations
# ---------------------------------------------------------------------------

INVESTIGATION_DURATION = Histogram(
    "kubortex_investigation_duration_seconds",
    "Time spent on investigations",
    ["category", "outcome"],
    buckets=(5, 15, 30, 60, 120, 300, 600),
)

INVESTIGATION_CONFIDENCE = Histogram(
    "kubortex_investigation_confidence",
    "Confidence scores of completed investigations",
    ["category"],
    buckets=(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.85, 0.9, 0.95, 1.0),
)

INVESTIGATION_CLAIMS = Counter(
    "kubortex_investigation_claims_total",
    "Investigation claim attempts",
    ["result"],
)

# ---------------------------------------------------------------------------
# Actions
# ---------------------------------------------------------------------------

ACTIONS_EXECUTED = Counter(
    "kubortex_actions_executed_total",
    "Total actions executed",
    ["type", "result"],
)

ACTIONS_DENIED = Counter(
    "kubortex_actions_denied_total",
    "Actions denied by policy engine",
    ["type", "reason"],
)

# ---------------------------------------------------------------------------
# Approvals
# ---------------------------------------------------------------------------

APPROVAL_WAIT = Histogram(
    "kubortex_approval_wait_seconds",
    "Time waiting for approval decisions",
    ["outcome"],
    buckets=(30, 60, 120, 300, 600, 1800, 3600),
)

# ---------------------------------------------------------------------------
# Budgets
# ---------------------------------------------------------------------------

BUDGET_REMAINING = Gauge(
    "kubortex_budget_remaining",
    "Remaining budget capacity",
    ["profile", "budget_type"],
)

# ---------------------------------------------------------------------------
# Skills
# ---------------------------------------------------------------------------

SKILL_INVOCATIONS = Counter(
    "kubortex_skill_invocations_total",
    "Total skill invocations",
    ["skill", "status"],
)

SKILL_LATENCY = Histogram(
    "kubortex_skill_latency_seconds",
    "Skill execution latency",
    ["skill"],
    buckets=(0.1, 0.5, 1, 2, 5, 10, 30, 60),
)

# ---------------------------------------------------------------------------
# LLM
# ---------------------------------------------------------------------------

LLM_CALLS = Counter(
    "kubortex_llm_calls_total",
    "Total LLM API calls",
    ["model", "status"],
)

LLM_LATENCY = Histogram(
    "kubortex_llm_latency_seconds",
    "LLM API call latency",
    ["model"],
    buckets=(0.5, 1, 2, 5, 10, 30, 60, 120),
)

# ---------------------------------------------------------------------------
# Payload store
# ---------------------------------------------------------------------------

PAYLOAD_STORE_SIZE = Gauge(
    "kubortex_payload_store_size_bytes",
    "Total size of the payload store on disk",
)

# ---------------------------------------------------------------------------
# Diagnostic learning
# ---------------------------------------------------------------------------

DIAGNOSTIC_SCORE_UPDATES = Counter(
    "kubortex_diagnostic_score_updates_total",
    "Diagnostic score update events",
    ["category", "target_kind"],
)
