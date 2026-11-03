from __future__ import annotations

from durable_actors.langchain import DurableActorChatMessageHistory
from durable_actors.langgraph import DurableActorSaver
from langchain_core.messages import HumanMessage
from langgraph.types import Command

from agent import build_workflow


def test_showcase_workflow_end_to_end() -> None:
    saver = DurableActorSaver.create_in_memory()
    history = DurableActorChatMessageHistory.create_in_memory("test-session")

    prompt = "Design distributed checkpointing"
    history.add_user_message(prompt)
    assert len(history.messages) == 1

    graph = build_workflow(checkpointer=saver)
    config = {"configurable": {"thread_id": "test-thread-1"}}

    # First turn: run until interrupt
    graph.invoke(
        {
            "topic": prompt,
            "messages": [HumanMessage(content=prompt)],
            "revision_count": 0,
            "approved": False,
        },
        config=config,
    )
    state = graph.get_state(config)
    assert state.next == ("human_review",)

    # Resume with approval
    final_output = graph.invoke(Command(resume=True), config=config)
    assert final_output["approved"] is True
    assert final_output["revision_count"] >= 2
    assert "Architecture specification" in final_output["draft"]

    # History checkpoint verification
    history_entries = list(graph.get_state_history(config))
    assert len(history_entries) >= 5
