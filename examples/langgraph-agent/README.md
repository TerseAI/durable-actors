# Multi-Agent LangGraph Workflow with Durable Actors

First-class LangGraph checkpoint persistence and LangChain conversational memory backed by typed, isolated Durable Actors.

[Quickstart](../../sdk-python/README.md#quickstart) · [Reference](../../docs/reference/python-guide.md) · [Agent](agent.py) · [Actors](actors.py) · [Main](main.py)

## Overview

This example demonstrates how to integrate **LangGraph** and **LangChain** with **Durable Actors**:

1. **`DurableActorSaver` (LangGraph Checkpoint Saver)**:
   - Persists execution state, multi-agent turns, and super-step snapshots.
   - Maps each LangGraph `thread_id` to an isolated Durable Actor (`LangGraphCheckpointActor`).
   - Supports monotonic versioning, parent chain traversal, and pending writes.
   - Emits checkpoint lifecycle events over WebSockets for live observability.

2. **`DurableActorChatMessageHistory` (LangChain Memory)**:
   - Stores conversational history in a dedicated `ChatMessageActor`.
   - Uses `emitted(persisted(...))` for real-time WebSocket synchronization with connected frontends.
   - Provides out-of-band message broadcast capability.

3. **Multi-Agent Collaboration & HITL**:
   - Multi-agent pipeline with **Researcher**, **Writer**, and **Critic** agents coordinating state.
   - Iterative revision loops based on automated critiques.
   - **Human-in-the-loop (HITL)** interrupt checkpoints and resumption via `Command(resume=...)`.
   - **Time-Travel & State Recovery**: Inspect historical checkpoints and rewind/fork graph execution from previous states.

## Architecture

```
                      +---------------------------------------+
                      |       DurableActorSaver               |
                      |  (LangGraph BaseCheckpointSaver)     |
                      +-------------------+-------------------+
                                          |
                +-------------------------+-------------------------+
                |                                                   |
                v                                                   v
      Thread "thread-alpha"                                Thread "thread-beta"
+-------------------------------+                   +-------------------------------+
|   LangGraphCheckpointActor    |                   |   LangGraphCheckpointActor    |
| - Checkpoint snapshots        |                   | - Checkpoint snapshots        |
| - Channel version blobs       |                   | - Channel version blobs       |
| - Ordered pending writes      |                   | - Ordered pending writes      |
| - WebSocket state broadcasts  |                   | - WebSocket state broadcasts  |
+-------------------------------+                   +-------------------------------+
```

## Run

Install Python 3.11+ and uv.

### 1. Run the showcase script

```sh
cd examples/langgraph-agent
uv run main.py
```

The script runs end-to-end:
- Initializes `DurableActorChatMessageHistory` and records the input prompt.
- Builds the multi-agent `StateGraph` with `DurableActorSaver`.
- Executes researcher, writer, and critic agents until reaching the human review interrupt.
- Resumes execution with supervisor approval.
- Inspects checkpoint history stored in the Durable Actor and demonstrates time-travel state recovery.

### 2. Connect to a Live Durable Actors Cluster

To run against a local or remote Durable Actors runtime:

```sh
# Start the durable-actors runtime
uv run durable-actors dev

# In another terminal, run with the control plane URL
export DURABLE_ACTORS_CONTROL_PLANE_URL=http://127.0.0.1:7100
uv run main.py
```

## Using in Your Projects

Install `durable-actors` with the `langgraph` and `langchain` extras:

```sh
pip install "durable-actors[langgraph,langchain]"
```

### Checkpoint Persistence with LangGraph

```python
from durable_actors.langgraph import DurableActorSaver
from langgraph.graph import StateGraph

builder = StateGraph(MyState)
# ... define nodes and edges ...

# Compile with DurableActorSaver
saver = DurableActorSaver()  # or DurableActorSaver.create_in_memory()
graph = builder.compile(checkpointer=saver)

# Run with thread_id for isolated actor persistence
config = {"configurable": {"thread_id": "session-42"}}
result = graph.invoke({"input": "Hello"}, config=config)
```

### Chat Message History with LangChain

```python
from durable_actors.langchain import DurableActorChatMessageHistory
from langchain_core.messages import HumanMessage, AIMessage

history = DurableActorChatMessageHistory("user-session-1")
history.add_user_message("Hello!")
history.add_ai_message("Hi there! How can I help you today?")

for msg in history.messages:
    print(f"{msg.type}: {msg.content}")
```
