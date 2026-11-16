from __future__ import annotations

import os

from durable_actors.langchain import DurableActorChatMessageHistory
from durable_actors.langgraph import DurableActorSaver
from langchain_core.messages import HumanMessage
from langgraph.types import Command

from agent import build_workflow


def run_showcase() -> None:
    print("=== Durable Actors LangGraph Multi-Agent Showcase ===")

    # Initialize checkpointer and chat message history
    # Can connect to live control-plane or run in-memory for testing
    control_plane = os.environ.get("DURABLE_ACTORS_CONTROL_PLANE_URL")
    if control_plane:
        print(f"Connecting to Durable Actors control plane at {control_plane}...")
        saver = DurableActorSaver()
        history = DurableActorChatMessageHistory("session-showcase")
    else:
        print("Using in-memory Durable Actor transport for local demonstration...")
        saver = DurableActorSaver.create_in_memory()
        history = DurableActorChatMessageHistory.create_in_memory("session-showcase")

    # Step 1: Record initial user prompt in persistent chat history
    user_prompt = "Design high-availability distributed state persistence for AI agents"
    history.add_user_message(user_prompt)
    print(
        f"\n[1] Recorded initial prompt in DurableActorChatMessageHistory: '{user_prompt}'"
    )

    # Step 2: Compile multi-agent workflow with DurableActorSaver
    graph = build_workflow(checkpointer=saver)
    thread_id = "agent-thread-alpha"
    config = {"configurable": {"thread_id": thread_id}}

    print(f"\n[2] Executing multi-agent workflow on thread '{thread_id}'...")
    initial_input = {
        "topic": user_prompt,
        "messages": [HumanMessage(content=user_prompt)],
        "revision_count": 0,
        "approved": False,
    }

    # Workflow runs until reaching human review interrupt
    graph.invoke(initial_input, config=config)

    print("\n--- Workflow paused at Human-in-the-Loop Interrupt ---")
    current_state = graph.get_state(config)
    print(f"Next step: {current_state.next}")
    if current_state.tasks and current_state.tasks[0].interrupts:
        prompt_data = current_state.tasks[0].interrupts[0].value
        print(f"Interrupt Prompt: {prompt_data}")

    # Step 3: Human supervisor reviews and resumes execution
    print("\n[3] Supervisor granting approval and resuming execution...")
    resume_command = Command(resume=True)
    final_output = graph.invoke(resume_command, config=config)

    print("\n--- Workflow Execution Completed ---")
    print(f"Final Approval Status: {final_output['approved']}")
    print(f"Final Draft: {final_output['draft']}")
    print(f"Total Messages in Turn: {len(final_output['messages'])}")

    # Record final outcome in chat history
    history.add_ai_message(final_output["draft"])

    # Step 4: Time-Travel & State History Inspection
    print("\n[4] Inspecting durable checkpoint history (Time-Travel)...")
    history_entries = list(graph.get_state_history(config))
    print(f"Total checkpoints stored in Durable Actor: {len(history_entries)}")
    for i, state_item in enumerate(history_entries):
        cid = state_item.config["configurable"]["checkpoint_id"]
        step = state_item.metadata.get("step", 0)
        writes = state_item.metadata.get("writes", {})
        print(
            f"  Checkpoint [{i}]: ID={cid[:12]}... Step={step} Writes={list(writes.keys())}"
        )

    # Recover state from an earlier checkpoint
    if len(history_entries) >= 2:
        earlier_checkpoint = history_entries[-2]
        print(
            f"\nRecovered state from earlier checkpoint {earlier_checkpoint.config['configurable']['checkpoint_id'][:12]}..."
        )
        print(
            f"Earlier revision count: {earlier_checkpoint.values.get('revision_count', 0)}"
        )

    print("\n=== Showcase completed successfully! ===")


if __name__ == "__main__":
    run_showcase()
