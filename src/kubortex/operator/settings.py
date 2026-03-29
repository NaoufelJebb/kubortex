"""Shared constants and single settings instance for the operator component.

Kopf decorator arguments are evaluated at import time, so GROUP, VERSION, and
timer intervals must be module-level constants derived from OperatorSettings.
All handler files import from here to eliminate duplication.
"""

from __future__ import annotations

from kubortex.shared.config import OperatorSettings
from kubortex.shared.constants import (  # noqa: F401 — re-exported for handler imports
    ACTION_EXECUTIONS,
    APPROVAL_REQUESTS,
    AUTONOMY_PROFILES,
    INCIDENTS,
    INVESTIGATIONS,
    REMEDIATION_PLANS,
)

settings = OperatorSettings()

GROUP = settings.crd_group
VERSION = settings.crd_version
