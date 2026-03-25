"""Shared enums and literal types for all Kubortex CRD fields."""

from __future__ import annotations

from enum import StrEnum

# ---------------------------------------------------------------------------
# Signal & Incident classification
# ---------------------------------------------------------------------------


class Severity(StrEnum):
    INFO = "info"
    WARNING = "warning"
    HIGH = "high"
    CRITICAL = "critical"


class Category(StrEnum):
    RESOURCE_SATURATION = "resource-saturation"
    ERROR_RATE = "error-rate"
    LATENCY = "latency"
    AVAILABILITY = "availability"
    DEPLOYMENT = "deployment"
    SECURITY = "security"
    CAPACITY = "capacity"
    CUSTOM = "custom"


# ---------------------------------------------------------------------------
# Per-resource phase enums
# ---------------------------------------------------------------------------


class IncidentPhase(StrEnum):
    DETECTED = "Detected"
    INVESTIGATING = "Investigating"
    REMEDIATION_PLANNED = "RemediationPlanned"
    PENDING_APPROVAL = "PendingApproval"
    EXECUTING = "Executing"
    RESOLVED = "Resolved"
    FAILED = "Failed"
    ESCALATED = "Escalated"
    SUPPRESSED = "Suppressed"


class InvestigationPhase(StrEnum):
    PENDING = "Pending"
    IN_PROGRESS = "InProgress"
    COMPLETED = "Completed"
    TIMED_OUT = "TimedOut"
    CANCELLED = "Cancelled"


class RemediationPlanPhase(StrEnum):
    PROPOSED = "Proposed"
    APPROVED = "Approved"
    EXECUTING = "Executing"
    COMPLETED = "Completed"
    REJECTED = "Rejected"
    FAILED = "Failed"
    SUPERSEDED = "Superseded"


class ApprovalRequestPhase(StrEnum):
    PENDING = "Pending"
    APPROVED = "Approved"
    REJECTED = "Rejected"
    TIMED_OUT = "TimedOut"


class ActionExecutionPhase(StrEnum):
    APPROVED = "Approved"
    EXECUTING = "Executing"
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"
    ROLLED_BACK = "RolledBack"


# ---------------------------------------------------------------------------
# Action & policy types
# ---------------------------------------------------------------------------


class ActionType(StrEnum):
    RESTART_POD = "restart-pod"
    SCALE_UP = "scale-up"
    ROLLBACK_DEPLOYMENT = "rollback-deployment"
    CORDON_NODE = "cordon-node"
    DRAIN_NODE = "drain-node"


class RiskTier(StrEnum):
    LOW = "low"
    HIGH = "high"
    CRITICAL = "critical"


class ApprovalLevel(StrEnum):
    NONE = "none"
    REQUIRED = "required"


class DecisionType(StrEnum):
    APPROVED = "approved"
    REJECTED = "rejected"
