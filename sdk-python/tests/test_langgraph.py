from __future__ import annotations

import operator
from typing import Annotated

import pytest
from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, interrupt
from typing_extensions import TypedDict

from durable_actors.contract import public_contract
from durable_actors.langgraph import DurableActorSaver, LangGraphCheckpointActor
from durable_actors.testing import InMemoryActorTransport


class SimpleState(TypedDict):
    messages: Annotated[list[str], operator.add]
    counter: int


def test_checkpoint_actor_contract() -> None:
    contract = public_contract([LangGraphCheckpointActor])
    actor_def = contract["actors"][0]
    assert actor_def["actorName"] == "LangGraphCheckpointActor"
    method_names = [m["name"] for m in actor_def["rpc"]["methods"]]
    assert "put_checkpoint" in method_names
    assert "get_checkpoint" in method_names
    assert "list_checkpoints" in method_names
    assert "put_writes" in method_names
    assert "delete_thread" in method_names


def test_checkpoint_actor_direct_storage() -> None:
    actor = LangGraphCheckpointActor()
    actor.put_checkpoint(
        checkpoint_ns="",
        checkpoint_id="chk-1",
        parent_checkpoint_id=None,
        checkpoint_blob=["json", "e30="],
        metadata_blob=["json", "e30="],
        channel_versions={"msg": 1},
        new_blobs={":msg:1": ["json", "WyJoZWxsbyJd"]},
    )
    res = actor.get_checkpoint("", "chk-1")
    assert res is not None
    assert res["checkpoint_id"] == "chk-1"
    assert "msg" in res["blobs"]

    # Latest checkpoint resolution
    latest = actor.get_checkpoint("")
    assert latest is not None
    assert latest["checkpoint_id"] == "chk-1"

    # Fallback when empty string passed
    latest_empty = actor.get_checkpoint("", "")
    assert latest_empty is not None
    assert latest_empty["checkpoint_id"] == "chk-1"

    # Pending writes
    actor.put_writes(
        "",
        "chk-1",
        [
            {
                "task_id": "t1",
                "idx": 0,
                "channel": "msg",
                "value": ["json", "WyJ3cml0ZSJd"],
                "task_path": "path1",
            }
        ],
    )
    with_writes = actor.get_checkpoint("", "chk-1")
    assert with_writes is not None
    assert len(with_writes["writes"]) == 1

    # Delete
    actor.delete_thread()
    assert actor.get_checkpoint("", "chk-1") is None


def test_put_writes_idempotency_and_special_indices() -> None:
    actor = LangGraphCheckpointActor()
    actor.put_checkpoint(
        checkpoint_ns="",
        checkpoint_id="chk-1",
        parent_checkpoint_id=None,
        checkpoint_blob=["json", "e30="],
        metadata_blob=["json", "e30="],
        channel_versions={"output": 1},
        new_blobs={":output:1": ["json", "WyJpbml0Il0="]},
    )

    # Initial write idx 0
    actor.put_writes(
        "",
        "chk-1",
        [
            {
                "task_id": "task-A",
                "idx": 0,
                "channel": "output",
                "value": ["json", "WyJmaXJzdCJd"],
                "task_path": "step1",
            }
        ],
    )

    # Duplicate write idx 0 with different value: should keep original (idempotent)
    actor.put_writes(
        "",
        "chk-1",
        [
            {
                "task_id": "task-A",
                "idx": 0,
                "channel": "output",
                "value": ["json", "WyJzZWNvbmQiXQ=="],
                "task_path": "step1",
            }
        ],
    )

    # Special negative index writes (e.g. interrupt / error): should append
    actor.put_writes(
        "",
        "chk-1",
        [
            {
                "task_id": "task-A",
                "idx": -3,
                "channel": "__interrupt__",
                "value": ["json", "WyJpbnRlcnJ1cHQxIl0="],
                "task_path": "step1",
            },
            {
                "task_id": "task-A",
                "idx": -3,
                "channel": "__interrupt__",
                "value": ["json", "WyJpbnRlcnJ1cHQyIl0="],
                "task_path": "step1",
            },
        ],
    )

    chk = actor.get_checkpoint("", "chk-1")
    assert chk is not None
    writes = chk["writes"]
    # 1 normal write + 2 negative index writes = 3 writes total
    assert len(writes) == 3
    # Normal write preserved original value (sorted by writes_sort_key: -3 sorts before 0)
    assert writes[2][1] == "output"
    assert writes[2][2] == ["json", "WyJmaXJzdCJd"]


def test_durable_saver_sync_roundtrip() -> None:
    saver = DurableActorSaver.create_in_memory()
    config = {"configurable": {"thread_id": "thread-sync-1", "checkpoint_ns": ""}}

    cp = {
        "v": 1,
        "id": "0001",
        "ts": "2026-01-01T00:00:00Z",
        "channel_values": {"items": ["alpha"]},
        "channel_versions": {"items": "0001.0"},
        "versions_seen": {},
    }
    meta = {"source": "test", "step": 1}

    saved_config = saver.put(config, cp, meta, {"items": "0001.0"})
    assert saved_config["configurable"]["checkpoint_id"] == "0001"

    tup = saver.get_tuple(saved_config)
    assert tup is not None
    assert tup.checkpoint["channel_values"]["items"] == ["alpha"]
    assert tup.metadata["step"] == 1

    # Put writes
    saver.put_writes(saved_config, [("items", "beta")], task_id="task-1", task_path="step1")
    tup_with_writes = saver.get_tuple(saved_config)
    assert tup_with_writes is not None
    assert len(tup_with_writes.pending_writes) == 1
    assert tup_with_writes.pending_writes[0] == ("task-1", "items", "beta")

    # List
    listed = list(saver.list(config))
    assert len(listed) == 1
    assert listed[0].config["configurable"]["checkpoint_id"] == "0001"

    # Delete
    saver.delete_thread("thread-sync-1")
    assert saver.get_tuple(saved_config) is None


@pytest.mark.asyncio
async def test_durable_saver_async_roundtrip() -> None:
    saver = DurableActorSaver.create_in_memory()
    config = {"configurable": {"thread_id": "thread-async-1", "checkpoint_ns": ""}}

    cp = {
        "v": 1,
        "id": "0002",
        "ts": "2026-01-01T00:00:00Z",
        "channel_values": {"score": 99},
        "channel_versions": {"score": "0002.0"},
        "versions_seen": {},
    }
    meta = {"source": "async-test"}

    saved_config = await saver.aput(config, cp, meta, {"score": "0002.0"})
    assert saved_config["configurable"]["checkpoint_id"] == "0002"

    tup = await saver.aget_tuple(saved_config)
    assert tup is not None
    assert tup.checkpoint["channel_values"]["score"] == 99

    await saver.aput_writes(
        saved_config, [("score", 100)], task_id="task-async", task_path="step_async"
    )
    tup_writes = await saver.aget_tuple(saved_config)
    assert tup_writes is not None
    assert len(tup_writes.pending_writes) == 1

    items: list[str] = []
    async for item in saver.alist(config):
        items.append(item.config["configurable"]["checkpoint_id"])
    assert items == ["0002"]

    await saver.adelete_thread("thread-async-1")
    assert await saver.aget_tuple(saved_config) is None


def test_durable_saver_metadata_filter_and_limit() -> None:
    saver = DurableActorSaver.create_in_memory()
    tid = "filter-test-thread"

    # 5 checkpoints: 2 newest are 'agent', 2 middle are 'human', 1 oldest is 'agent'
    entries = [
        ("0001", {"source": "agent", "step": 1}),
        ("0002", {"source": "human", "step": 2}),
        ("0003", {"source": "human", "step": 3}),
        ("0004", {"source": "agent", "step": 4}),
        ("0005", {"source": "agent", "step": 5}),
    ]
    for cid, meta in entries:
        cp = {
            "v": 1,
            "id": cid,
            "ts": "2026-01-01T00:00:00Z",
            "channel_values": {"x": 1},
            "channel_versions": {"x": f"{cid}.0"},
            "versions_seen": {},
        }
        saver.put(
            {"configurable": {"thread_id": tid, "checkpoint_ns": ""}},
            cp,
            meta,
            {"x": f"{cid}.0"},
        )

    config = {"configurable": {"thread_id": tid}}

    # Query with filter and limit: should find 0003 and 0002 even though 0005 and 0004 are newer
    human_results = list(saver.list(config, filter={"source": "human"}, limit=2))
    assert len(human_results) == 2
    assert [r.config["configurable"]["checkpoint_id"] for r in human_results] == ["0003", "0002"]

    # Limit <= 0 returns empty
    assert list(saver.list(config, limit=0)) == []
    assert list(saver.list(config, limit=-1)) == []


def test_durable_saver_missing_and_empty_config() -> None:
    saver = DurableActorSaver.create_in_memory()
    assert saver.get_tuple({}) is None
    assert saver.get_tuple({"configurable": {}}) is None
    assert saver.get_tuple({"configurable": {"thread_id": "nonexistent"}}) is None

    # put requires thread_id
    with pytest.raises(ValueError, match="thread_id"):
        saver.put(
            {"configurable": {}},
            {
                "v": 1,
                "id": "1",
                "ts": "",
                "channel_values": {},
                "channel_versions": {},
                "versions_seen": {},
            },
            {},
            {},
        )


def test_subgraph_namespace_isolation() -> None:
    saver = DurableActorSaver.create_in_memory()
    tid = "subgraph-thread"

    cp_main = {
        "v": 1,
        "id": "cp-main",
        "ts": "2026-01-01T00:00:00Z",
        "channel_values": {"scope": "root"},
        "channel_versions": {"scope": "1.0"},
        "versions_seen": {},
    }
    saver.put(
        {"configurable": {"thread_id": tid, "checkpoint_ns": ""}}, cp_main, {}, {"scope": "1.0"}
    )

    cp_sub = {
        "v": 1,
        "id": "cp-sub",
        "ts": "2026-01-01T00:00:00Z",
        "channel_values": {"scope": "subgraph"},
        "channel_versions": {"scope": "1.0"},
        "versions_seen": {},
    }
    saver.put(
        {"configurable": {"thread_id": tid, "checkpoint_ns": "sub_ns"}},
        cp_sub,
        {},
        {"scope": "1.0"},
    )

    # Retrieve root namespace
    tup_root = saver.get_tuple({"configurable": {"thread_id": tid, "checkpoint_ns": ""}})
    assert tup_root is not None
    assert tup_root.checkpoint["channel_values"]["scope"] == "root"

    # Retrieve sub namespace
    tup_sub = saver.get_tuple({"configurable": {"thread_id": tid, "checkpoint_ns": "sub_ns"}})
    assert tup_sub is not None
    assert tup_sub.checkpoint["channel_values"]["scope"] == "subgraph"

    # List with specific ns
    assert (
        len(list(saver.list({"configurable": {"thread_id": tid, "checkpoint_ns": "sub_ns"}}))) == 1
    )


def test_cross_thread_discovery() -> None:
    transport = InMemoryActorTransport()
    saver1 = DurableActorSaver(transport=transport, async_transport=transport.as_async())

    saver1.put(
        {"configurable": {"thread_id": "thread-alpha", "checkpoint_ns": ""}},
        {
            "v": 1,
            "id": "a1",
            "ts": "",
            "channel_values": {},
            "channel_versions": {},
            "versions_seen": {},
        },
        {},
        {},
    )
    saver1.put(
        {"configurable": {"thread_id": "thread-beta", "checkpoint_ns": ""}},
        {
            "v": 1,
            "id": "b1",
            "ts": "",
            "channel_values": {},
            "channel_versions": {},
            "versions_seen": {},
        },
        {},
        {},
    )

    # A separate saver instance with empty local _known_thread_ids
    saver2 = DurableActorSaver(transport=transport, async_transport=transport.as_async())
    discovered = list(saver2.list(None))
    assert len(discovered) == 2
    cids = {t.config["configurable"]["checkpoint_id"] for t in discovered}
    assert cids == {"a1", "b1"}


def test_durable_saver_with_langgraph_workflow() -> None:
    builder = StateGraph(SimpleState)

    def node_researcher(state: SimpleState) -> dict[str, list[str] | int]:
        return {
            "messages": [f"researcher note {state.get('counter', 0)}"],
            "counter": state.get("counter", 0) + 1,
        }

    def node_reviewer(state: SimpleState) -> dict[str, list[str]]:
        return {"messages": ["reviewer approved"]}

    builder.add_node("researcher", node_researcher)
    builder.add_node("reviewer", node_reviewer)
    builder.add_edge(START, "researcher")
    builder.add_edge("researcher", "reviewer")
    builder.add_edge("reviewer", END)

    with DurableActorSaver.create_in_memory() as checkpointer:
        graph = builder.compile(checkpointer=checkpointer)

        # Thread 1 turn 1
        t1_cfg = {"configurable": {"thread_id": "conv-1"}}
        res1 = graph.invoke({"messages": ["init"], "counter": 0}, config=t1_cfg)
        assert res1["messages"] == ["init", "researcher note 0", "reviewer approved"]
        assert res1["counter"] == 1

        # Thread 1 turn 2 (state accumulates)
        res2 = graph.invoke({"messages": ["follow-up"]}, config=t1_cfg)
        assert len(res2["messages"]) == 6
        assert res2["messages"] == [
            "init",
            "researcher note 0",
            "reviewer approved",
            "follow-up",
            "researcher note 1",
            "reviewer approved",
        ]
        assert res2["counter"] == 2

        # Thread 2 isolation
        t2_cfg = {"configurable": {"thread_id": "conv-2"}}
        res_t2 = graph.invoke({"messages": ["isolated"], "counter": 10}, config=t2_cfg)
        assert res_t2["messages"] == ["isolated", "researcher note 10", "reviewer approved"]
        assert res_t2["counter"] == 11


def test_durable_saver_human_in_the_loop_interrupt() -> None:
    class HitlState(TypedDict):
        count: int
        approved: bool

    builder = StateGraph(HitlState)

    def step_prompt(state: HitlState) -> dict[str, int]:
        return {"count": state["count"] + 1}

    def step_human_review(state: HitlState) -> dict[str, bool]:
        decision = interrupt({"prompt": "Approve execution?"})
        return {"approved": bool(decision)}

    builder.add_node("prompt", step_prompt)
    builder.add_node("review", step_human_review)
    builder.add_edge(START, "prompt")
    builder.add_edge("prompt", "review")
    builder.add_edge("review", END)

    saver = DurableActorSaver.create_in_memory()
    graph = builder.compile(checkpointer=saver)

    config = {"configurable": {"thread_id": "hitl-thread-1"}}

    # Pauses at interrupt
    output1 = graph.invoke({"count": 5, "approved": False}, config=config)
    assert "__interrupt__" in output1

    # Check state before resume
    state1 = graph.get_state(config)
    assert state1.next == ("review",)

    # Resume with approval
    output2 = graph.invoke(Command(resume=True), config=config)
    assert output2["approved"] is True
    assert output2["count"] == 6


def test_durable_saver_time_travel_and_state_recovery() -> None:
    builder = StateGraph(SimpleState)

    def increment(state: SimpleState) -> dict[str, int | list[str]]:
        c = state.get("counter", 0) + 1
        return {"counter": c, "messages": [f"step {c}"]}

    builder.add_node("inc", increment)
    builder.add_edge(START, "inc")
    builder.add_edge("inc", END)

    saver = DurableActorSaver.create_in_memory()
    graph = builder.compile(checkpointer=saver)

    config = {"configurable": {"thread_id": "time-travel-1"}}
    graph.invoke({"counter": 0, "messages": []}, config=config)
    graph.invoke(None, config=config)
    graph.invoke(None, config=config)

    history = list(graph.get_state_history(config))
    assert len(history) >= 3

    # Recover state from an earlier checkpoint
    earlier = history[-2]
    assert earlier.values["counter"] < history[0].values["counter"]

    # Resume/fork from earlier state
    forked = graph.invoke(None, config=earlier.config)
    assert forked is not None


def test_version_generation_is_monotonic() -> None:
    saver = DurableActorSaver.create_in_memory()
    v1 = saver.get_next_version(None, None)
    v2 = saver.get_next_version(v1, None)
    v3 = saver.get_next_version(v2, None)
    assert v1 < v2 < v3
