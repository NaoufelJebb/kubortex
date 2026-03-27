"""CRD plural resource name constants shared across all Kubortex components.

These literals map to the ``plural`` field in each CustomResourceDefinition
and are used by every component that talks to the Kubernetes API.
"""

from __future__ import annotations

INCIDENTS = "incidents"
INVESTIGATIONS = "investigations"
REMEDIATION_PLANS = "remediationplans"
APPROVAL_REQUESTS = "approvalrequests"
ACTION_EXECUTIONS = "actionexecutions"
AUTONOMY_PROFILES = "autonomyprofiles"
