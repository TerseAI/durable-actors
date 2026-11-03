from __future__ import annotations

import operator
from collections.abc import Sequence
from typing import Annotated, Any, Literal

from langchain_core.messages import AIMessage, BaseMessage, HumanMessage
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.graph import END, START, StateGraph
from langgraph.graph.state import CompiledStateGraph
from langgraph.types import interrupt
from typing_extensions import TypedDict


class MultiAgentState(TypedDict):
    """Shared state across the research, writing, and review agents."""

    messages: Annotated[Sequence[BaseMessage], operator.add]
    topic: str
    research_notes: str
    draft: str
    critique: str
    revision_count: int
    approved: bool


def researcher_agent(state: MultiAgentState) -> dict[str, Any]:
    """Analyze the topic and extract key technical requirements."""
    topic = state.get("topic", "Durable Distributed Execution")
    notes = (
        f"Key requirements for {topic}: 1) Isolated state per thread, "
        f"2) Monotonic checkpointing, 3) WebSocket emission, 4) Human approval."
    )
    msg = AIMessage(content=f"[Researcher]: Compiled findings for {topic}.")
    return {"research_notes": notes, "messages": [msg]}


def writer_agent(state: MultiAgentState) -> dict[str, Any]:
    """Draft technical summary based on research notes and any prior critique."""
    notes = state.get("research_notes", "")
    critique = state.get("critique", "")
    rev = state.get("revision_count", 0) + 1
    draft = f"Architecture specification (rev {rev}) based on: {notes}"
    if critique:
        draft += f" | Incorporating feedback: {critique}"
    msg = AIMessage(content=f"[Writer]: Prepared draft revision {rev}.")
    return {"draft": draft, "revision_count": rev, "messages": [msg]}


def critic_agent(state: MultiAgentState) -> dict[str, Any]:
    """Evaluate technical draft and decide whether revisions or approval are needed."""
    rev = state.get("revision_count", 1)
    if rev < 2:
        critique = "Add explicit error recovery details."
        msg = AIMessage(content=f"[Critic]: Revision {rev} needs detail. {critique}")
        return {"critique": critique, "messages": [msg]}

    msg = AIMessage(
        content="[Critic]: Draft revision meets quality criteria. Ready for human approval."
    )
    return {"critique": "Approved by automated critic.", "messages": [msg]}


def should_continue(state: MultiAgentState) -> Literal["writer", "human_review"]:
    """Route back to writer if critique requires changes, else forward to human review."""
    rev = state.get("revision_count", 1)
    if rev < 2:
        return "writer"
    return "human_review"


def human_review_node(state: MultiAgentState) -> dict[str, Any]:
    """Interrupt execution and wait for human supervisor approval."""
    draft = state.get("draft", "")
    decision = interrupt(
        {"question": "Approve publication of architecture draft?", "draft": draft}
    )
    approved = bool(decision)
    status_str = (
        "Approved by human operator" if approved else "Rejected by human operator"
    )
    msg = HumanMessage(content=f"[Human Supervisor]: {status_str}.")
    return {"approved": approved, "messages": [msg]}


def build_workflow(
    checkpointer: BaseCheckpointSaver[str] | None = None,
) -> CompiledStateGraph:
    """Build and compile the multi-agent graph with checkpoint persistence."""
    builder = StateGraph(MultiAgentState)

    builder.add_node("researcher", researcher_agent)
    builder.add_node("writer", writer_agent)
    builder.add_node("critic", critic_agent)
    builder.add_node("human_review", human_review_node)

    builder.add_edge(START, "researcher")
    builder.add_edge("researcher", "writer")
    builder.add_edge("writer", "critic")
    builder.add_conditional_edges("critic", should_continue, ["writer", "human_review"])
    builder.add_edge("human_review", END)

    return builder.compile(checkpointer=checkpointer)
