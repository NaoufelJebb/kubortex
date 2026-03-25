"""Kopf handler for AutonomyProfile CRD validation and budget resets."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import kopf
import structlog

from kubortex.shared.k8s import patch_status
from kubortex.shared.models.autonomy import AutonomyProfileSpec

logger = structlog.get_logger(__name__)

GROUP = "kubortex.io"
VERSION = "v1alpha1"
AUTONOMY_PROFILES = "autonomyprofiles"


@kopf.on.create(GROUP, VERSION, AUTONOMY_PROFILES)
@kopf.on.update(GROUP, VERSION, AUTONOMY_PROFILES)
async def on_autonomy_profile_upsert(
    body: dict[str, Any],
    name: str,
    **_: Any,
) -> None:
    """Validate the AutonomyProfile spec on create/update."""
    try:
        AutonomyProfileSpec.model_validate(body.get("spec", {}))
        logger.info("autonomy_profile_valid", name=name)
    except Exception as exc:
        logger.error("autonomy_profile_invalid", name=name, error=str(exc))
        raise kopf.PermanentError(f"Invalid AutonomyProfile: {exc}") from exc


@kopf.timer(GROUP, VERSION, AUTONOMY_PROFILES, interval=60)
async def reset_budget_counters(
    body: dict[str, Any],
    name: str,
    namespace: str,
    **_: Any,
) -> None:
    """Periodically reset hourly/daily budget counters."""
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
