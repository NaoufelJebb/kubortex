"""Graph nodes for the LangGraph ReAct investigation loop.

Nodes: initialise, reason, invoke, summarise, conclude.
"""

from __future__ import annotations

import json
import re
from typing import Any

import structlog
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

from kubortex.investigator.context.assembler import ContextAssembler
from kubortex.investigator.context.compression import apply_compression
from kubortex.investigator.payload.store import PayloadStore
from kubortex.investigator.skills.gateway import CapabilityGateway
from kubortex.investigator.skills.models import SkillInput
from kubortex.shared.config import InvestigatorSettings

from .state import InvestigationState

logger = structlog.get_logger(__name__)


async def initialise(
    state: InvestigationState,
    *,
    assembler: ContextAssembler,
) -> dict[str, Any]:
    """Build initial context from investigation spec and inject Layer 0 prompt."""
    from kubortex.investigator.prompts import load_prompt

    ctx = state["incident_context"]
    hints = ctx.get("diagnosticHints")
    prompt = assembler.build_initial_prompt(ctx, diagnostic_hints=hints)

    system_msg = SystemMessage(content=load_prompt("SYSTEM_PROMPT.md") + "\n\n" + prompt)
    human_msg = HumanMessage(
        content=(
            f"Investigate this incident: {ctx.get('summary', 'Unknown')}. "
            f"Category: {ctx.get('category')}, Severity: {ctx.get('severity')}. "
            "Use available skills to gather evidence and determine root cause."
        )
    )

    return {
        "messages": [system_msg, human_msg],
        "iteration": 0,
        "evidence": [],
        "loaded_skills": set(),
        "loaded_runbook": False,
        "force_conclude": False,
    }


async def reason(
    state: InvestigationState,
    *,
    llm: Any,
) -> dict[str, Any]:
    """Core LLM call — decides whether to invoke a skill or conclude."""
    messages = state["messages"]
    response = await llm.ainvoke(messages)
    return {"messages": [response], "iteration": state["iteration"] + 1}


async def invoke(
    state: InvestigationState,
    *,
    gateway: CapabilityGateway,
    assembler: ContextAssembler,
) -> dict[str, Any]:
    """Route the LLM's tool call through the Capability Gateway."""
    messages = state["messages"]
    last_msg = messages[-1]

    if not isinstance(last_msg, AIMessage) or not last_msg.tool_calls:
        return {}

    tool_call = last_msg.tool_calls[0]
    skill_name = tool_call["name"]
    args = tool_call.get("args", {})

    updates: dict[str, Any] = {}

    injected_ids: list[str] = list(state.get("injected_message_ids", []))
    # Lazy-load skill body (Layer 1) — inject into messages so LLM sees documentation
    body = assembler.inject_skill_body(skill_name)
    if body:
        logger.debug("skill_body_loaded", skill=skill_name)
        skill_msg = HumanMessage(content=f"## Skill: {skill_name}\n\n{body}")
        updates["messages"] = [skill_msg]
        updates["loaded_skills"] = state.get("loaded_skills", set()) | {skill_name}
        injected_ids.append(skill_msg.id)

    # Lazy-load runbook body (Layer 2) on first invoke
    if state.get("matched_runbook") and not state.get("loaded_runbook"):
        rb_body = assembler.inject_runbook_body(state["matched_runbook"])
        if rb_body:
            logger.debug("runbook_body_loaded", runbook=state["matched_runbook"])
            rb_msg = HumanMessage(content=f"## Runbook Strategy\n\n{rb_body}")
            updates.setdefault("messages", []).append(rb_msg)
            updates["loaded_runbook"] = True
            injected_ids.append(rb_msg.id)

    if injected_ids != list(state.get("injected_message_ids", [])):
        updates["injected_message_ids"] = injected_ids

    inp = SkillInput(
        query=args.get("query", ""),
        namespace=args.get("namespace", ""),
        parameters=args.get("parameters", {}),
    )

    result, invocation_record = await gateway.invoke(skill_name, inp)
    result_text = result.summary if result.success else f"Error: {result.error}"
    updates["skill_records"] = list(state.get("skill_records", [])) + [
        invocation_record.model_dump(by_alias=True)
    ]

    from langchain_core.messages import ToolMessage

    tool_msg = ToolMessage(
        content=result_text,
        tool_call_id=tool_call["id"],
    )
    updates.setdefault("messages", []).append(tool_msg)

    # Carry the raw result for payload writing in summarise
    updates["_last_result"] = result

    return updates


async def summarise(
    state: InvestigationState,
    *,
    assembler: ContextAssembler,
    payload_store: PayloadStore,
    settings: InvestigatorSettings,
) -> dict[str, Any]:
    """Compress the skill result and store full payload externally."""
    messages = state["messages"]
    evidence = list(state.get("evidence", []))
    seq = state.get("seq", 0)

    # Extract the last tool result summary
    if messages and hasattr(messages[-1], "content"):
        summary = str(messages[-1].content)[: settings.evidence_summary_max_chars]
        evidence_item: dict[str, Any] = {"valueSummary": summary}

        # Persist full payload to store and record reference
        last_result = state.get("_last_result")
        if last_result is not None and last_result.data is not None:
            raw = (
                last_result.data
                if isinstance(last_result.data, dict)
                else {"raw": last_result.data}
            )
            payload_store.write(
                state["incident_name"],
                state["investigation_name"],
                seq,
                raw,
            )
            evidence_item["payloadRef"] = (
                f"{state['incident_name']}/{state['investigation_name']}/{seq}"
            )

        evidence.append(evidence_item)
        assembler.add_evidence(summary)

    # Apply progressive compression if budget is under pressure
    evidence, force_conclude, ids_to_evict = apply_compression(
        budget=assembler.budget,
        evidence=evidence,
        loaded_skills=set(state.get("loaded_skills", set())),
        loaded_runbook=state.get("loaded_runbook", False),
        messages=messages,
        injected_message_ids=state.get("injected_message_ids", []),
    )

    from langgraph.graph.message import RemoveMessage

    result: dict[str, Any] = {
        "evidence": evidence,
        "seq": seq + 1,
        "force_conclude": force_conclude,
    }
    if ids_to_evict:
        result["messages"] = [RemoveMessage(id=mid) for mid in ids_to_evict]
        result["injected_message_ids"] = []
    return result


async def conclude(
    state: InvestigationState,
    *,
    llm: Any,
) -> dict[str, Any]:
    """Produce the final InvestigationResult as structured output."""
    from kubortex.investigator.prompts import load_prompt

    messages = list(state["messages"])
    conclude_msg = HumanMessage(content=load_prompt("CONCLUDE_PROMPT.md"))
    messages.append(conclude_msg)

    response = await llm.ainvoke(messages)
    result = _parse_conclusion(response.content)
    return {"messages": [response], "result": result}


def _parse_conclusion(content: str) -> dict[str, Any]:
    """Best-effort parse of the LLM's JSON conclusion."""
    m = re.search(r"```(?:json)?\s*([\s\S]+?)```", content)
    if m:
        content = m.group(1).strip()

    try:
        return json.loads(content)
    except json.JSONDecodeError:
        logger.warning("conclusion_parse_failed",
                       content_preview=content[:200])
        return {
            "hypothesis": "Unable to parse structured conclusion",
            "confidence": 0.0,
            "escalate": True,
            "escalationReason": "Failed to produce structured output",
        }
