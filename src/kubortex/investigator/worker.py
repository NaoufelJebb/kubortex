"""Investigator worker — watches Investigation CRs, claims, runs the graph, writes results.

The worker polls for Pending investigations, claims them via optimistic
concurrency, executes the LangGraph ReAct graph, and patches the result
back into the Investigation status.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import structlog
from langchain_core.tools import StructuredTool

from kubortex.investigator.context.assembler import ContextAssembler
from kubortex.investigator.graph.builder import build_investigation_graph
from kubortex.investigator.learning.feedback import record_feedback
from kubortex.investigator.learning.scorer import StrategyRanker
from kubortex.investigator.learning.store import LearningStore
from kubortex.investigator.payload.store import PayloadStore
from kubortex.investigator.runbooks.matcher import match_runbook
from kubortex.investigator.runbooks.registry import RunbookRegistry
from kubortex.investigator.skills.gateway import CapabilityGateway
from kubortex.investigator.skills.models import SkillInput
from kubortex.investigator.skills.registry import SkillRegistry
from kubortex.shared.config import InvestigatorSettings
from kubortex.shared.constants import INVESTIGATIONS
from kubortex.shared.crds import list_resources, patch_status, try_claim
from kubortex.shared.metrics import (
    INVESTIGATION_CLAIMS,
    INVESTIGATION_CONFIDENCE,
    INVESTIGATION_DURATION,
)
from kubortex.shared.models.investigation import InvestigationResult
from kubortex.shared.types import InvestigationPhase

logger = structlog.get_logger(__name__)


class InvestigatorWorker:
    """Watches for pending Investigations and runs the ReAct graph."""

    def __init__(self, settings: InvestigatorSettings) -> None:
        self._settings = settings

        import sys
        from pathlib import Path
        skills_parent = str(Path(settings.skills_dir).parent)
        if skills_parent not in sys.path:
            sys.path.insert(0, skills_parent)
        self._skill_registry = SkillRegistry()
        self._skill_registry.load(settings.skills_dir)

        self._runbook_registry = RunbookRegistry()
        self._runbook_registry.load(settings.runbooks_dir)

        self._learning_store = LearningStore(settings.learning_store_path)
        self._ranker = StrategyRanker(self._learning_store, settings)
        self._payload_store = PayloadStore(settings)

        # Long-lived gateway — reset per-investigation invocation counts each run
        self._gateway = CapabilityGateway(registry=self._skill_registry)

    async def run(self) -> None:
        """Main polling loop — runs until cancelled."""
        logger.info("investigator_worker_started", pod=self._settings.pod_name)

        # Clean stale payloads on startup to prevent unbounded PVC growth
        self._payload_store.gc()

        while True:
            try:
                await self._poll_and_process()
            except asyncio.CancelledError:
                logger.info("investigator_worker_stopped")
                return
            except Exception:
                logger.exception("poll_cycle_error")

            await asyncio.sleep(self._settings.poll_interval_seconds)

    async def _poll_and_process(self) -> None:
        """Find pending Investigations, claim one, and run it."""
        # TODO(perf): list_resources fetches all Investigations regardless of
        # phase and filters client-side.  Once the operator sets a
        # ``kubortex.io/phase`` label on create/transition, switch to
        # label_selector="kubortex.io/phase=Pending" to reduce API load.
        investigations = await list_resources(INVESTIGATIONS)
        pending = [
            inv
            for inv in investigations
            if (inv.get("status") or {}).get("phase") == InvestigationPhase.PENDING
            and not (inv.get("status") or {}).get("claimedBy")
        ]

        if not pending:
            return

        for inv in pending:
            name = inv["metadata"]["name"]
            claimed = await try_claim(INVESTIGATIONS, name, self._settings.pod_name)
            INVESTIGATION_CLAIMS.labels(result="won" if claimed else "lost").inc()
            if claimed:
                await self._run_investigation(inv)
                break

    async def _run_investigation(self, inv: dict[str, Any]) -> None:
        """Execute the full investigation graph for a claimed Investigation CR."""
        name = inv["metadata"]["name"]
        spec = inv.get("spec", {})

        incident_name = spec.get("incidentRef", "unknown")

        logger.info("investigation_started", name=name, incident=incident_name)

        graph_start = time.monotonic()
        try:
            await patch_status(INVESTIGATIONS, name, {"phase": InvestigationPhase.IN_PROGRESS})

            categories = spec.get("categories") or []
            if not isinstance(categories, list):
                categories = []
            category = next((item for item in categories if isinstance(item, str) and item), "")
            severity = spec.get("severity", "")
            target = spec.get("targetRef", {})

            # Per-investigation config from CRD spec (operator-written), fall back to settings
            max_iterations = spec.get("maxIterations", self._settings.max_iterations)
            timeout_seconds = spec.get("timeoutSeconds", self._settings.timeout_seconds)

            hints = self._ranker.get_hints(category, target.get("kind", "") if target else "")
            matched = match_runbook(self._runbook_registry, category, severity)

            assembler = ContextAssembler(
                skill_registry=self._skill_registry,
                runbook_registry=self._runbook_registry,
                max_tokens=self._settings.context_max_tokens,
                model=self._settings.llm_model,
            )

            self._gateway.reset_counts()

            llm = self._build_llm()
            tools = self._build_tools()
            llm_with_tools = llm.bind_tools(tools) if tools else llm

            graph = build_investigation_graph(
                llm=llm_with_tools,
                gateway=self._gateway,
                assembler=assembler,
                payload_store=self._payload_store,
                settings=self._settings,
            )

            initial_state = {
                "messages": [],
                "incident_context": {
                    "summary": spec.get("summary", ""),
                    "category": category,
                    "severity": severity,
                    "targetRef": target,
                    "signals": spec.get("signals", []),
                    "diagnosticHints": hints,
                    "matchedRunbook": matched.name if matched else None,
                    "priorAttempts": spec.get("priorAttempts", []),
                },
                "evidence": [],
                "iteration": 0,
                "seq": 0,
                "loaded_skills": set(),
                "matched_runbook": matched.name if matched else None,
                "loaded_runbook": False,
                "force_conclude": False,
                "skill_records": [],
                "injected_message_ids": [],
                "investigation_name": name,
                "incident_name": incident_name,
                "max_iterations": max_iterations,
            }

            result_state = await asyncio.wait_for(
                graph.ainvoke(initial_state),
                timeout=timeout_seconds,
            )

            result = result_state.get("result", {})
            telemetry = {
                "iterationsUsed": result_state.get("iteration", 0),
                "skillInvocations": result_state.get("skill_records", []),
            }

            await patch_status(
                INVESTIGATIONS,
                name,
                {
                    "result": result,
                    "telemetry": telemetry,
                    "selectedRunbook": matched.name if matched else None,
                },
            )

            # Record feedback for the learning system
            try:
                inv_result = InvestigationResult.model_validate(result)
                resolved = (
                    not result.get("escalate", True)
                    and result.get("confidence", 0.0) >= 0.60
                )
                record_feedback(
                    ranker=self._ranker,
                    result=inv_result,
                    category=category,
                    target_kind=target.get("kind", "") if target else "",
                    resolved=resolved,
                )
            except Exception:
                logger.exception("feedback_recording_failed", name=name)

            duration = time.monotonic() - graph_start
            INVESTIGATION_DURATION.labels(category=category, outcome="completed").observe(duration)
            confidence = result.get("confidence", 0.0)
            INVESTIGATION_CONFIDENCE.labels(category=category).observe(confidence)

            logger.info(
                "investigation_completed",
                name=name,
                confidence=confidence,
                escalate=result.get("escalate"),
            )

        except TimeoutError:
            INVESTIGATION_DURATION.labels(category=category, outcome="timed_out").observe(
                time.monotonic() - graph_start
            )
            logger.warning("investigation_timed_out", name=name)
            await patch_status(
                INVESTIGATIONS,
                name,
                {
                    # AIDEV-NOTE: Timeout/failure paths set an explicit phase
                    # and still persist a structured result so the operator and
                    # downstream readers can distinguish terminal worker errors
                    # from successful result-driven completion.
                    "phase": InvestigationPhase.TIMED_OUT,
                    "result": {
                        "hypothesis": "",
                        "confidence": 0.0,
                        "recommendedActions": [],
                        "evidence": [],
                        "escalate": True,
                        "escalationReason": "Investigation exceeded timeout",
                    }
                },
            )
        except Exception:
            INVESTIGATION_DURATION.labels(category=category, outcome="failed").observe(
                time.monotonic() - graph_start
            )
            logger.exception("investigation_failed", name=name)
            await patch_status(
                INVESTIGATIONS,
                name,
                {
                    "phase": InvestigationPhase.FAILED,
                    "result": {
                        "hypothesis": "",
                        "confidence": 0.0,
                        "recommendedActions": [],
                        "evidence": [],
                        "escalate": True,
                        "escalationReason": "Internal investigator error",
                    }
                },
            )

    def _build_llm(self) -> Any:
        """Construct the LLM client based on settings."""
        provider = self._settings.llm_provider
        if provider != "openai":
            raise ValueError(
                f"Unsupported llm_provider {provider!r}; only 'openai' is "
                f"currently implemented"
            )
        from langchain_openai import ChatOpenAI

        return ChatOpenAI(
            model=self._settings.llm_model,
            api_key=self._settings.llm_api_key,
            temperature=self._settings.llm_temperature,
            max_tokens=self._settings.llm_max_tokens,
            timeout=self._settings.llm_timeout_seconds,
        )

    def _build_tools(self) -> list[StructuredTool]:
        """Convert skill metadata into LangChain tools for LLM binding.

        The lambda body is intentionally a no-op — actual routing is handled
        by the invoke node through the CapabilityGateway.  SkillInput provides
        the args schema so the LLM produces well-formed tool calls.
        """
        tools = []
        for meta in self._skill_registry.list_metadata():
            tool = StructuredTool.from_function(
                func=lambda **kwargs: None,
                name=meta["name"],
                description=meta["description"],
                args_schema=SkillInput,
            )
            tools.append(tool)
        return tools
