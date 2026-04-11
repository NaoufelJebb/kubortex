"""Kopf handlers for AutonomyProfile validation and periodic budget window resets.

On create/update the profile spec is validated through the Pydantic model
and rejected as a ``kopf.PermanentError`` on failure. A timer handler rolls
over the per-hour and per-day budget counters whenever their windows
expire, so policy evaluations always see up-to-date usage.
"""

from __future__ import annotations

from typing import Any

import kopf
import structlog

from kubortex.operator.budget import reset_if_needed
from kubortex.operator.settings import GROUP, VERSION, settings
from kubortex.shared.constants import AUTONOMY_PROFILES
from kubortex.shared.crds import patch_status
from kubortex.shared.models.autonomy import AutonomyProfileSpec, BudgetUsage

logger = structlog.get_logger(__name__)


@kopf.on.create(GROUP, VERSION, AUTONOMY_PROFILES)
@kopf.on.update(GROUP, VERSION, AUTONOMY_PROFILES)
async def on_autonomy_profile_upsert(
    body: dict[str, Any],
    name: str,
    **_: Any,
) -> None:
    """Validate an AutonomyProfile spec on create or update.

    Parses the full spec through ``AutonomyProfileSpec`` (Pydantic v2). Any
    validation error — unknown fields, wrong types, out-of-range values — is
    surfaced as a ``kopf.PermanentError`` so kopf marks the event as
    permanently failed and does not retry. The operator logs the error but
    takes no other action; the profile remains in place with its previous
    valid state until the user corrects and re-applies it.

    Args:
        body: AutonomyProfile resource body.
        name: AutonomyProfile name.

    Raises:
        kopf.PermanentError: Raised when ``spec`` fails Pydantic validation.
    """
    try:
        AutonomyProfileSpec.model_validate(body.get("spec", {}))
        logger.info("autonomy_profile_valid", name=name)
    except Exception as exc:
        logger.error("autonomy_profile_invalid", name=name, error=str(exc))
        raise kopf.PermanentError(f"Invalid AutonomyProfile: {exc}") from exc


@kopf.timer(GROUP, VERSION, AUTONOMY_PROFILES, interval=settings.budget_reset_interval)
async def reset_budget_counters(
    body: dict[str, Any],
    name: str,
    namespace: str,
    **_: Any,
) -> None:
    """Roll over per-hour and per-day budget counters when their windows expire.

    Delegates to ``operator.budget.reset_if_needed`` — the single source of
    truth for hour/day rollover logic. The timer handler validates the raw
    ``status.budgetUsage`` through Pydantic, runs the reset, and only patches
    when the result differs from what was read. This avoids an unconditional
    first-run patch when nothing legitimately changed.

    Args:
        body: AutonomyProfile resource body.
        name: AutonomyProfile name.
        namespace: AutonomyProfile namespace.
    """
    raw = (body.get("status") or {}).get("budgetUsage", {})
    current = BudgetUsage.model_validate(raw)
    updated = reset_if_needed(current)

    if updated == current:
        return

    await patch_status(
        AUTONOMY_PROFILES,
        name,
        {"budgetUsage": updated.model_dump(by_alias=True)},
    )
    logger.debug("budget_counters_reset", profile=name)
