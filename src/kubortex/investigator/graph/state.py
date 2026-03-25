"""Investigation graph state definition for LangGraph."""

from __future__ import annotations

from typing import Annotated, Any

from langgraph.graph import add_messages
from typing_extensions import TypedDict


class InvestigationState(TypedDict):
    """State flowing through the LangGraph investigation graph."""

    # LangGraph message list (append-only via add_messages reducer)
    messages: Annotated[list[Any], add_messages]

    # Denormalised investigation spec data
    incident_context: dict[str, Any]

    # Accumulated evidence items (summaries only)
    evidence: list[dict[str, Any]]

    # Current iteration count
    iteration: int

    # Context budget remaining (approximate chars)
    context_budget_remaining: int

    # Set of skill names whose full bodies have been injected
    loaded_skills: set[str]

    # Selected runbook metadata or None
    matched_runbook: str | None

    # Whether the full runbook strategy has been loaded
    loaded_runbook: bool

    # Investigation spec reference for result writing
    investigation_name: str

    # Max iterations allowed
    max_iterations: int
