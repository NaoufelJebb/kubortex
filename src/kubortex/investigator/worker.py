"""Investigator worker — watches Investigation CRs, claims, runs the graph, writes results.

The worker polls for Pending investigations, claims them via optimistic
concurrency, executes the LangGraph ReAct graph, and patches the result
back into the Investigation status.
"""

from __future__ import annotations

import asyncio
import os
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
from kubortex.investigator.skills.registry import SkillRegistry
from kubortex.shared.config import KubortexSettings
from kubortex.shared.k8s import list_resources, patch_status, try_claim

logger = structlog.get_logger(__name__)

POD_NAME = os.environ.get("POD_NAME", "investigator-0")
POLL_INTERVAL = 5  # seconds


class InvestigatorWorker:
    """Watches for pending Investigations and runs the ReAct graph."""

    def __init__(self, settings: KubortexSettings) -> None:
        self._settings = settings
        self._skill_registry = SkillRegistry(settings.skills_dir)
        self._runbook_registry = RunbookRegistry(settings.runbooks_dir)
        self._learning_store = LearningStore(settings.learning_store_path)
        self._ranker = StrategyRanker(self._learning_store)
        self._payload_store = PayloadStore()

    async def run(self) -> None:
        """Main polling loop — runs until cancelled."""
        logger.info("investigator_worker_started", pod=POD_NAME)

        while True:
            try:
                await self._poll_and_process()
            except asyncio.CancelledError:
                logger.info("investigator_worker_stopped")
                return
            except Exception:
                logger.exception("poll_cycle_error")

            await asyncio.sleep(POLL_INTERVAL)

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

        # Try to claim the first available
        for inv in pending:
            name = inv["metadata"]["name"]
            claimed = await try_claim("investigations", name, POD_NAME)
            if claimed:
                await self._run_investigation(inv)
                break

    async def _run_investigation(self, inv: dict[str, Any]) -> None:
        """Execute the full investigation graph for a claimed Investigation CR."""
        name = inv["metadata"]["name"]
        spec = inv.get("spec", {})
        incident_ref = spec.get("incidentRef", {})
        incident_name = incident_ref.get("name", "unknown")

        logger.info("investigation_started", name=name, incident=incident_name)

        try:
            # Transition to InProgress
            await patch_status("investigations", name, {"phase": "InProgress"})

            # Build context
            category = spec.get("category", "")
            severity = spec.get("severity", "")
            target = spec.get("targetRef", {})

            # Get diagnostic hints from learning
            hints = self._ranker.get_hints(category, target.get("kind", ""))

            # Match runbook
            matched = match_runbook(
                self._runbook_registry,
                category,
                severity,
                spec.get("labels", {}),
            )

            # Build assembler
            assembler = ContextAssembler(
                skill_registry=self._skill_registry,
                runbook_registry=self._runbook_registry,
            )

            # Build gateway
            gateway = CapabilityGateway(registry=self._skill_registry)

            # Build LLM (dynamic import to avoid hard dependency)
            llm = self._build_llm()

            # Bind skill tools to LLM
            tools = self._build_tools()
            llm_with_tools = llm.bind_tools(tools) if tools else llm

            # Build and run graph
            graph = build_investigation_graph(
                llm=llm_with_tools,
                gateway=gateway,
                assembler=assembler,
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
                "context_budget_remaining": self._settings.context_max_chars,
                "loaded_skills": set(),
                "matched_runbook": matched.name if matched else None,
                "loaded_runbook": False,
                "investigation_name": name,
                "max_iterations": self._settings.investigator_max_iterations,
            }

            result_state = await graph.ainvoke(initial_state)

            # Extract result and write to status
            result = result_state.get("result", {})
            await patch_status(
                "investigations",
                name,
                {
                    "phase": "Completed",
                    "result": result,
                },
            )

            # Record feedback for learning
            record_feedback(
                ranker=self._ranker,
                category=category,
                target_kind=target.get("kind", ""),
                diagnostic_path=result.get("diagnosticPath", []),
                confidence=result.get("confidence", 0.0),
            )

            logger.info(
                "investigation_completed",
                name=name,
                confidence=result.get("confidence"),
                escalate=result.get("escalate"),
            )

        except Exception:
            logger.exception("investigation_failed", name=name)
            await patch_status(
                "investigations",
                name,
                {
                    "phase": "Failed",
                    "error": "Internal investigator error",
                },
            )

    def _build_llm(self) -> Any:
        """Construct the LLM client based on settings."""
        from langchain_openai import ChatOpenAI

        return ChatOpenAI(
            model=self._settings.llm_model,
            api_key=self._settings.llm_api_key,
            temperature=0,
            max_tokens=self._settings.llm_max_tokens,
            timeout=self._settings.llm_timeout_seconds,
        )

    def _build_tools(self) -> list[StructuredTool]:
        """Convert skill metadata into LangChain tools for LLM binding."""
        tools = []
        for meta in self._skill_registry.list_metadata():
            tool = StructuredTool.from_function(
                func=lambda **kwargs: None,  # placeholder — routing handled by gateway
                name=meta.name,
                description=meta.description,
                args_schema=None,
            )
            tools.append(tool)
        return tools
