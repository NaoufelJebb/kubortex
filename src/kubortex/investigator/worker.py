"""Investigator worker — watches Investigation CRs, claims, runs the graph, writes results.

The worker polls for Pending investigations, claims them via optimistic
concurrency, executes the LangGraph ReAct graph, and patches the result
back into the Investigation status.
"""

from __future__ import annotations

import asyncio
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
from kubortex.shared.crds import list_resources, patch_status, try_claim
from kubortex.shared.models.investigation import InvestigationResult

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
        investigations = await list_resources("investigations")
        pending = [
            inv
            for inv in investigations
            if (inv.get("status") or {}).get("phase") == "Pending"
            and not (inv.get("status") or {}).get("claimedBy")
        ]

        if not pending:
            return

        for inv in pending:
            name = inv["metadata"]["name"]
            claimed = await try_claim("investigations", name, self._settings.pod_name)
            if claimed:
                await self._run_investigation(inv)
                break

    async def _run_investigation(self, inv: dict[str, Any]) -> None:
        """Execute the full investigation graph for a claimed Investigation CR."""
        name = inv["metadata"]["name"]
        spec = inv.get("spec", {})

        # incidentRef is a plain string in InvestigationSpec
        incident_name = spec.get("incidentRef", "unknown")

        logger.info("investigation_started", name=name, incident=incident_name)

        try:
            await patch_status("investigations", name, {"phase": "InProgress"})

            category = spec.get("category", "")
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
                },
                "evidence": [],
                "iteration": 0,
                "seq": 0,
                "context_budget_remaining": self._settings.context_max_tokens,
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
                "investigations",
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
                resolved = not result.get("escalate", True) and result.get("confidence", 0.0) >= 0.60
                record_feedback(
                    ranker=self._ranker,
                    result=inv_result,
                    category=category,
                    target_kind=target.get("kind", "") if target else "",
                    resolved=resolved,
                )
            except Exception:
                logger.exception("feedback_recording_failed", name=name)

            logger.info(
                "investigation_completed",
                name=name,
                confidence=result.get("confidence"),
                escalate=result.get("escalate"),
            )

        except asyncio.TimeoutError:
            logger.warning("investigation_timed_out", name=name)
            await patch_status(
                "investigations",
                name,
                {
                    # AIDEV-NOTE: Timeout/failure paths set an explicit phase
                    # and still persist a structured result so the operator and
                    # downstream readers can distinguish terminal worker errors
                    # from successful result-driven completion.
                    "phase": "TimedOut",
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
            logger.exception("investigation_failed", name=name)
            await patch_status(
                "investigations",
                name,
                {
                    "phase": "Failed",
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
