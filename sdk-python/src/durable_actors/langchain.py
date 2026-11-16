"""LangChain chat message history backed by durable actors with WebSocket broadcast support."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any, Self, cast

from .actor import Actor
from .fields import emitted, persisted
from .json import JsonValue

if TYPE_CHECKING:
    from .async_client import AsyncActorTransport, AsyncRpcTransport
    from .client import ActorTransport, RpcTransport

try:
    from langchain_core.chat_history import BaseChatMessageHistory
    from langchain_core.messages import (
        BaseMessage,
        message_to_dict,
        messages_from_dict,
    )
except ImportError as error:
    raise ImportError(
        "langchain-core is required to use DurableActorChatMessageHistory. "
        "Install it with 'pip install durable-actors[langchain]' or 'pip install langchain-core'."
    ) from error


class ChatMessageActor(Actor[JsonValue, JsonValue, JsonValue, str]):
    """Durable Actor maintaining chat history with WebSocket emission and broadcast support."""

    messages: list[dict[str, JsonValue]] = emitted(
        persisted(default_factory=cast(Callable[[], list[dict[str, JsonValue]]], list))
    )

    def get_messages(self) -> list[dict[str, JsonValue]]:
        """Return the current conversation message history."""
        return self.messages

    def add_messages(
        self, new_messages: list[dict[str, JsonValue]], broadcast: bool = True
    ) -> list[dict[str, JsonValue]]:
        """Append messages to persistent history and broadcast to connected sockets."""
        self.messages.extend(new_messages)
        if broadcast:
            for msg in new_messages:
                self._safe_broadcast(msg)
        return self.messages

    def broadcast_message(self, message: dict[str, JsonValue]) -> None:
        """Broadcast a message over WebSockets without adding it to history."""
        self._safe_broadcast(message)

    def clear(self) -> None:
        """Clear message history and notify connected sockets."""
        self.messages.clear()
        self._safe_broadcast({"type": "history_cleared"})

    def _safe_broadcast(self, message: JsonValue) -> None:
        try:
            self.broadcast(message)
        except (LookupError, ValueError, RuntimeError):
            pass


class DurableActorChatMessageHistory(BaseChatMessageHistory):
    """LangChain chat message history backed by a durable actor with WebSocket broadcast support."""

    def __init__(
        self,
        session_id: str,
        *,
        actor_name: str = "ChatMessageActor",
        transport: ActorTransport | RpcTransport | None = None,
        async_transport: AsyncActorTransport | AsyncRpcTransport | None = None,
        broadcast: bool = True,
    ) -> None:
        super().__init__()
        self.session_id = session_id
        self.actor_name = actor_name
        self.broadcast = broadcast
        self._transport = transport
        self._async_transport = async_transport

    @classmethod
    def create_in_memory(
        cls,
        session_id: str,
        *,
        actor_name: str = "ChatMessageActor",
        broadcast: bool = True,
    ) -> Self:
        """Create a chat history instance with an in-memory transport for testing."""
        from .testing import InMemoryActorTransport

        mem = InMemoryActorTransport()
        return cls(
            session_id,
            actor_name=actor_name,
            transport=mem,
            async_transport=mem.as_async(),
            broadcast=broadcast,
        )

    @property
    def transport(self) -> RpcTransport:
        if self._transport is None:
            from .client import default_client

            self._transport = default_client()
        return self._transport

    @property
    def async_transport(self) -> AsyncRpcTransport:
        if self._async_transport is None:
            from .async_client import default_async_client

            self._async_transport = default_async_client()
        return self._async_transport

    @property
    def messages(self) -> list[BaseMessage]:  # pyright: ignore[reportIncompatibleVariableOverride]
        """Retrieve stored chat messages."""
        raw: list[dict[str, Any]] = self.transport.invoke(
            self.actor_name, self.session_id, "get_messages", []
        )
        if not raw:
            return []
        return messages_from_dict(raw)

    @messages.setter
    def messages(  # pyright: ignore[reportIncompatibleVariableOverride]
        self, value: list[BaseMessage]
    ) -> None:
        """Replace stored chat messages."""
        self.clear()
        self.add_messages(value)

    async def aget_messages(self) -> list[BaseMessage]:
        """Asynchronously retrieve stored chat messages."""
        raw: list[dict[str, Any]] = await self.async_transport.invoke(
            self.actor_name, self.session_id, "get_messages", []
        )
        if not raw:
            return []
        return messages_from_dict(raw)

    def add_messages(self, messages: Sequence[BaseMessage]) -> None:
        """Persist messages and broadcast them over WebSockets."""
        payload = [message_to_dict(m) for m in messages]
        self.transport.invoke(
            self.actor_name, self.session_id, "add_messages", [payload, self.broadcast]
        )

    async def aadd_messages(self, messages: Sequence[BaseMessage]) -> None:
        """Asynchronously persist messages and broadcast them over WebSockets."""
        payload = [message_to_dict(m) for m in messages]
        await self.async_transport.invoke(
            self.actor_name, self.session_id, "add_messages", [payload, self.broadcast]
        )

    def clear(self) -> None:
        """Clear all messages from the store."""
        self.transport.invoke(self.actor_name, self.session_id, "clear", [])

    async def aclear(self) -> None:
        """Asynchronously clear all messages from the store."""
        await self.async_transport.invoke(self.actor_name, self.session_id, "clear", [])

    def broadcast_message(self, message: BaseMessage) -> None:
        """Send an out-of-band broadcast to connected WebSockets."""
        payload = message_to_dict(message)
        if hasattr(self.transport, "broadcast"):
            getattr(self.transport, "broadcast")(self.actor_name, self.session_id, payload)
        else:
            self.transport.invoke(self.actor_name, self.session_id, "broadcast_message", [payload])

    async def abroadcast_message(self, message: BaseMessage) -> None:
        """Asynchronously send an out-of-band broadcast to connected WebSockets."""
        payload = message_to_dict(message)
        if hasattr(self.async_transport, "broadcast"):
            await getattr(self.async_transport, "broadcast")(
                self.actor_name, self.session_id, payload
            )
        else:
            await self.async_transport.invoke(
                self.actor_name, self.session_id, "broadcast_message", [payload]
            )
