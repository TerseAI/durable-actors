from __future__ import annotations

from typing import Any

import pytest
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

from durable_actors.client import RpcTransport
from durable_actors.contract import public_contract
from durable_actors.langchain import ChatMessageActor, DurableActorChatMessageHistory
from durable_actors.testing import InMemoryActorTransport


class PureRpcTransport(RpcTransport):
    """Transport that implements only RpcTransport without direct ActorTransport.broadcast."""

    def __init__(self, backend: InMemoryActorTransport) -> None:
        self._backend = backend

    def invoke(self, actor_name: str, actor_id: str, method: str, args: list[Any]) -> Any:
        return self._backend.invoke(actor_name, actor_id, method, args)


def test_chat_message_actor_contract() -> None:
    contract = public_contract([ChatMessageActor])
    actor_def = contract["actors"][0]
    assert actor_def["actorName"] == "ChatMessageActor"
    method_names = [m["name"] for m in actor_def["rpc"]["methods"]]
    assert "get_messages" in method_names
    assert "add_messages" in method_names
    assert "broadcast_message" in method_names
    assert "clear" in method_names
    assert "messages" in actor_def["socket"]["emittable"]


def test_chat_message_actor_direct() -> None:
    actor = ChatMessageActor()
    assert actor.get_messages() == []
    actor.add_messages([{"type": "human", "data": {"content": "hi"}}], broadcast=False)
    msgs = actor.get_messages()
    assert len(msgs) == 1
    assert msgs[0]["type"] == "human"

    # Out-of-band broadcast does not mutate history
    actor.broadcast_message({"type": "ping"})
    assert len(actor.get_messages()) == 1

    actor.clear()
    assert actor.get_messages() == []


def test_durable_chat_history_sync_operations() -> None:
    history = DurableActorChatMessageHistory.create_in_memory("sess-sync-1")
    assert history.messages == []

    history.add_user_message("Hello from user")
    history.add_ai_message("Hello from AI")

    msgs = history.messages
    assert len(msgs) == 2
    assert isinstance(msgs[0], HumanMessage)
    assert msgs[0].content == "Hello from user"
    assert isinstance(msgs[1], AIMessage)
    assert msgs[1].content == "Hello from AI"

    # Replace via property setter
    history.messages = [SystemMessage(content="You are a helpful assistant.")]
    replaced = history.messages
    assert len(replaced) == 1
    assert isinstance(replaced[0], SystemMessage)
    assert replaced[0].content == "You are a helpful assistant."

    history.clear()
    assert history.messages == []


@pytest.mark.asyncio
async def test_durable_chat_history_async_operations() -> None:
    history = DurableActorChatMessageHistory.create_in_memory("sess-async-1")
    assert await history.aget_messages() == []

    await history.aadd_messages(
        [
            HumanMessage(content="Async human"),
            AIMessage(content="Async ai"),
        ]
    )

    msgs = await history.aget_messages()
    assert len(msgs) == 2
    assert msgs[0].content == "Async human"
    assert msgs[1].content == "Async ai"

    await history.aclear()
    assert await history.aget_messages() == []


def test_durable_chat_history_websocket_broadcast() -> None:
    transport = InMemoryActorTransport()
    history = DurableActorChatMessageHistory(
        "sess-broadcast-1",
        transport=transport,
        async_transport=transport.as_async(),
        broadcast=True,
    )

    history.broadcast_message(HumanMessage(content="Broadcast message 1"))
    broadcasts = transport.get_broadcasts(actor_id="sess-broadcast-1")
    assert len(broadcasts) >= 1
    assert broadcasts[0]["message"]["data"]["content"] == "Broadcast message 1"


@pytest.mark.asyncio
async def test_durable_chat_history_async_websocket_broadcast() -> None:
    transport = InMemoryActorTransport()
    history = DurableActorChatMessageHistory(
        "sess-broadcast-async-1",
        transport=transport,
        async_transport=transport.as_async(),
        broadcast=True,
    )

    await history.abroadcast_message(AIMessage(content="Broadcast async message"))
    broadcasts = transport.get_broadcasts(actor_id="sess-broadcast-async-1")
    assert len(broadcasts) >= 1
    assert broadcasts[0]["message"]["data"]["content"] == "Broadcast async message"


def test_durable_chat_history_in_actor_broadcast_and_clear() -> None:
    transport = InMemoryActorTransport()
    history = DurableActorChatMessageHistory(
        "sess-in-actor-broadcast",
        transport=transport,
        broadcast=True,
    )

    history.add_user_message("Broadcasted turn")
    broadcasts = transport.get_broadcasts(actor_id="sess-in-actor-broadcast")
    assert len(broadcasts) >= 1
    assert broadcasts[-1]["message"]["data"]["content"] == "Broadcasted turn"

    history.clear()
    clear_broadcasts = [
        b
        for b in transport.get_broadcasts(actor_id="sess-in-actor-broadcast")
        if isinstance(b["message"], dict) and b["message"].get("type") == "history_cleared"
    ]
    assert len(clear_broadcasts) == 1


def test_durable_chat_history_rpc_only_fallback_broadcast() -> None:
    mem = InMemoryActorTransport()
    rpc_transport = PureRpcTransport(mem)
    history = DurableActorChatMessageHistory(
        "sess-rpc-fallback",
        transport=rpc_transport,
        broadcast=True,
    )

    # Calling broadcast_message should invoke broadcast_message RPC without mutating history
    history.broadcast_message(SystemMessage(content="Out of band alert"))
    broadcasts = mem.get_broadcasts(actor_id="sess-rpc-fallback")
    assert len(broadcasts) >= 1
    assert broadcasts[-1]["message"]["data"]["content"] == "Out of band alert"
    # History messages remained empty!
    assert history.messages == []


def test_durable_chat_history_multi_session_isolation() -> None:
    history_a = DurableActorChatMessageHistory.create_in_memory("sess-A")
    history_b = DurableActorChatMessageHistory.create_in_memory("sess-B")

    history_a.add_user_message("Message in A")
    assert len(history_a.messages) == 1
    assert len(history_b.messages) == 0

    history_b.add_user_message("Message in B")
    assert len(history_b.messages) == 1
    assert history_a.messages[0].content == "Message in A"
    assert history_b.messages[0].content == "Message in B"
