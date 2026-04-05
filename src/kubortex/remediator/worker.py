"""Remediator worker — watches ActionExecution CRs and runs the action pipeline.

Pipeline: pre_flight → dry_run → execute → verify.
If verification detects regression, rollback is attempted.
"""

from __future__ import annotations

import asyncio
from typing import Any

import structlog

from kubortex.remediator.actions.registry import get_action
from kubortex.shared.config import RemediatorSettings
from kubortex.shared.crds import list_resources, patch_status, try_claim

logger = structlog.get_logger(__name__)


class RemediatorWorker:
    """Watches for Approved ActionExecutions and runs the remediation pipeline."""

    def __init__(self, settings: RemediatorSettings) -> None:
        self._settings = settings

    async def run(self) -> None:
        """Main polling loop — runs until cancelled."""
        logger.info("remediator_worker_started", pod=self._settings.pod_name)

        while True:
            try:
                await self._poll_and_process()
            except asyncio.CancelledError:
                logger.info("remediator_worker_stopped")
                return
            except Exception:
                logger.exception("poll_cycle_error")

            await asyncio.sleep(self._settings.poll_interval_seconds)

    async def _poll_and_process(self) -> None:
        """Find Approved ActionExecutions, claim all available, and run them concurrently."""
        executions = await list_resources("actionexecutions")
        approved = [
            ae
            for ae in executions
            if (ae.get("status") or {}).get("phase") == "Approved"
            and not (ae.get("status") or {}).get("claimedBy")
        ]

        if not approved:
            return

        tasks = []
        for ae in approved:
            name = ae["metadata"]["name"]
            claimed = await try_claim("actionexecutions", name, self._settings.pod_name)
            if claimed:
                tasks.append(self._run_action(ae))

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _run_action(self, ae: dict[str, Any]) -> None:
        """Execute the full pre_flight → dry_run → execute → verify pipeline."""
        name = ae["metadata"]["name"]
        spec = ae.get("spec", {})
        action_type = spec.get("action", {}).get("type", "")
        target = spec.get("action", {}).get("target", {})
        parameters = spec.get("action", {}).get("parameters", {})

        logger.info("action_execution_started", name=name, action=action_type)

        try:
            action = get_action(action_type)
        except KeyError:
            logger.error("unknown_action_type", type=action_type)
            await patch_status(
                "actionexecutions",
                name,
                {
                    "result": {"improved": False},
                    "error": f"Unknown action type: {action_type}",
                },
            )
            return

        try:
            # Pre-flight
            safe = await action.pre_flight(target, parameters)
            if not safe:
                await patch_status(
                    "actionexecutions",
                    name,
                    {
                        "result": {"improved": False},
                        "error": "Pre-flight check failed",
                    },
                )
                return

            # Dry-run (informational only — result not persisted)
            dry_result = await action.dry_run(target, parameters)
            logger.info("dry_run_complete", name=name, result=dry_result)

            # Execute
            exec_result = await action.execute(target, parameters)

            # Verify
            verification = await action.verify(target, parameters, exec_result)

            if verification.get("improved"):
                await patch_status(
                    "actionexecutions",
                    name,
                    {
                        "result": exec_result,
                        "verification": verification,
                    },
                )
                logger.info("action_succeeded", name=name)
            else:
                # Attempt rollback
                logger.warning("verification_failed", name=name, verification=verification)
                rollback_result = await action.rollback(target, parameters, exec_result)

                await patch_status(
                    "actionexecutions",
                    name,
                    {
                        "result": exec_result,
                        "verification": verification,
                        "rollback": {"triggered": True, **rollback_result},
                    },
                )
                logger.info("action_rolled_back", name=name)

        except Exception:
            logger.exception("action_execution_failed", name=name)
            await patch_status(
                "actionexecutions",
                name,
                {
                    "result": {"improved": False},
                    "error": "Internal remediator error",
                },
            )
