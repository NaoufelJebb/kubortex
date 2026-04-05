"""Build the LangGraph StateGraph for the investigation ReAct loop.

Graph: initialise → reason → (invoke → summarise → reason | conclude)
"""

from __future__ import annotations

from functools import partial
from typing import Any

from langchain_core.messages import AIMessage
from langgraph.graph import END, StateGraph

from kubortex.investigator.context.assembler import ContextAssembler
from kubortex.investigator.payload.store import PayloadStore
from kubortex.investigator.skills.gateway import CapabilityGateway
from kubortex.shared.config import InvestigatorSettings

from . import nodes
from .state import InvestigationState


def should_continue(state: InvestigationState) -> str:
    """Conditional edge: decide whether to invoke a skill or conclude."""
    # Compression stage 5 demands immediate conclusion
    if state.get("force_conclude"):
        return "conclude"

    iteration = state["iteration"]
    max_iter = state.get("max_iterations", 10)

    # Force conclusion if iteration budget exhausted
    if iteration >= max_iter:
        return "conclude"

    # Check if the last message is an AI message with tool calls
    messages = state["messages"]
    if messages:
        last = messages[-1]
        if isinstance(last, AIMessage) and last.tool_calls:
            return "invoke"

    return "conclude"


def build_investigation_graph(
    llm: Any,
    gateway: CapabilityGateway,
    assembler: ContextAssembler,
    payload_store: PayloadStore,
    settings: InvestigatorSettings,
) -> Any:
    """Construct and compile the LangGraph investigation graph."""
    graph = StateGraph(InvestigationState)

    graph.add_node("initialise", partial(nodes.initialise, assembler=assembler))
    graph.add_node("reason", partial(nodes.reason, llm=llm))
    graph.add_node(
        "invoke",
        partial(nodes.invoke, gateway=gateway, assembler=assembler),
    )
    graph.add_node(
        "summarise",
        partial(nodes.summarise, assembler=assembler, payload_store=payload_store, settings=settings),
    )
    graph.add_node("conclude", partial(nodes.conclude, llm=llm))

    graph.set_entry_point("initialise")
    graph.add_edge("initialise", "reason")
    graph.add_conditional_edges(
        "reason",
        should_continue,
        {
            "invoke": "invoke",
            "conclude": "conclude",
        },
    )
    graph.add_edge("invoke", "summarise")
    graph.add_edge("summarise", "reason")
    graph.add_edge("conclude", END)

    return graph.compile()
