"""Graph nodes for the LangGraph ReAct investigation loop.

Nodes: initialise, reason, invoke, summarise, conclude.
"""

from __future__ import annotations

import json
from typing import Any

import structlog
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

from kubortex.investigator.context.assembler import ContextAssembler
from kubortex.investigator.skills.gateway import CapabilityGateway
from kubortex.shared.models import SkillInput

from .prompts import INVESTIGATION_SYSTEM_PROMPT
from .state import InvestigationState

logger = structlog.get_logger(__name__)


async def initialise(
    state: InvestigationState,
    *,
    assembler: ContextAssembler,
) -> dict[str, Any]:
    """Build initial context from investigation spec and inject Layer 0 prompt."""
    ctx = state["incident_context"]
    prompt = assembler.build_initial_prompt(ctx)

    system_msg = SystemMessage(content=INVESTIGATION_SYSTEM_PROMPT + "\n\n" + prompt)
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

    # Lazy-load skill body (Layer 1)
    body = assembler.inject_skill_body(skill_name)
    if body:
        logger.debug("skill_body_loaded", skill=skill_name)

    inp = SkillInput(
        query=args.get("query", ""),
        namespace=args.get("namespace", ""),
        parameters=args.get("parameters", {}),
    )

    result, _record = await gateway.invoke(skill_name, inp)
    result_text = result.summary if result.success else f"Error: {result.error}"

    # Record as tool response message
    from langchain_core.messages import ToolMessage

    tool_msg = ToolMessage(
        content=result_text,
        tool_call_id=tool_call["id"],
    )

    return {"messages": [tool_msg]}


async def summarise(
    state: InvestigationState,
    *,
    assembler: ContextAssembler,
) -> dict[str, Any]:
    """Compress the skill result and store full payload externally."""
    messages = state["messages"]
    evidence = list(state.get("evidence", []))

    # Extract the last tool result
    if messages and hasattr(messages[-1], "content"):
        summary = str(messages[-1].content)[:2000]
        evidence.append({"valueSummary": summary})
        assembler.add_evidence(summary)

    return {"evidence": evidence}


async def conclude(
    state: InvestigationState,
    *,
    llm: Any,
) -> dict[str, Any]:
    """Produce the final InvestigationResult as structured output."""
    messages = list(state["messages"])
    conclude_prompt = HumanMessage(
        content=(
            "Based on all evidence gathered, produce your final investigation "
            "conclusion as JSON with these fields: hypothesis, confidence (0.0-1.0), "
            "reasoning, evidence (list of {skill, query, valueSummary, interpretation}), "
            "recommendedActions (list of {type, target: {kind, namespace, name}, "
            "parameters, rationale}), escalate (boolean), escalationReason (string|null), "
            "diagnosticPath (list of {skill, query, wasUseful})."
        )
    )
    messages.append(conclude_prompt)

    response = await llm.ainvoke(messages)

    # Parse the structured result from LLM response
    result = _parse_conclusion(response.content)
    return {"messages": [response], "result": result}


def _parse_conclusion(content: str) -> dict[str, Any]:
    """Best-effort parse of the LLM's JSON conclusion."""
    # Try to extract JSON from markdown code blocks
    if "```json" in content:
        start = content.index("```json") + 7
        end = content.index("```", start)
        content = content[start:end].strip()
    elif "```" in content:
        start = content.index("```") + 3
        end = content.index("```", start)
        content = content[start:end].strip()

    try:
        return json.loads(content)
    except json.JSONDecodeError:
        logger.warning("conclusion_parse_failed", content_preview=content[:200])
        return {
            "hypothesis": "Unable to parse structured conclusion",
            "confidence": 0.0,
            "escalate": True,
            "escalationReason": "Failed to produce structured output",
        }
