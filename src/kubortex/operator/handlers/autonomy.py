"""Kopf handler for AutonomyProfile CRD validation and budget resets."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import kopf
import structlog

from kubortex.operator.settings import GROUP, VERSION, settings
from kubortex.shared.constants import AUTONOMY_PROFILES
from kubortex.shared.k8s import patch_status
from kubortex.shared.models.autonomy import AutonomyProfileSpec

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

    Reads ``status.budgetUsage.lastResetHour`` and ``lastResetDay`` to decide
    whether a reset is due:
    - Hour window: resets ``podsKilledThisHour`` and ``scaleUpsThisHour``
      when ≥ 3600 seconds have elapsed since the last reset.
    - Day window: resets ``rollbacksToday`` when the calendar date has
      changed (UTC) since the last reset.

    On first run (no timestamp stored), the timestamps are initialised without
    zeroing the counters — the assumption is that any existing usage was
    accumulated in the current window.

    Only writes to the API when at least one counter was changed, avoiding
    unnecessary patch calls on every timer tick.

    Args:
        body: AutonomyProfile resource body.
        name: AutonomyProfile name.
        namespace: AutonomyProfile namespace.
    """
    status = body.get("status", {})
    usage = status.get("budgetUsage", {})
    now = datetime.now(UTC)
    changed = False

    last_hour_str = usage.get("lastResetHour")
    if last_hour_str:
        last_hour = datetime.fromisoformat(last_hour_str.replace("Z", "+00:00"))
        if (now - last_hour).total_seconds() >= 3600:
            usage["podsKilledThisHour"] = 0
            usage["scaleUpsThisHour"] = 0
            usage["lastResetHour"] = now.isoformat()
            changed = True
    else:
        usage["lastResetHour"] = now.isoformat()
        changed = True

    last_day_str = usage.get("lastResetDay")
    if last_day_str:
        last_day = datetime.fromisoformat(last_day_str.replace("Z", "+00:00"))
        if now.date() != last_day.date():
            usage["rollbacksToday"] = 0
            usage["lastResetDay"] = now.isoformat()
            changed = True
    else:
        usage["lastResetDay"] = now.isoformat()
        changed = True

    if changed:
        await patch_status(
            AUTONOMY_PROFILES,
            name,
            {"budgetUsage": usage},
            namespace=namespace,
        )
        logger.debug("budget_counters_reset", profile=name)
