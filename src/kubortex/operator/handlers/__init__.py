"""Kopf handler registration — import all handler modules so decorators activate."""

from kubortex.operator.handlers import (  # noqa: F401
    action,
    approval,
    autonomy,
    incident,
    investigation,
    remediation,
)
