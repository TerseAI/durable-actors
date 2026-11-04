"""In-memory actor transport for offline testing and development."""

from __future__ import annotations

import inspect
import time
from typing import Any, cast

from .actor import Actor
from .async_client import AsyncActorTransport
from .client import ActorTransport, SocketGrant
from .socket import scope_context


class _InMemoryMockScope:
    def __init__(
        self,
        transport: InMemoryActorTransport,
        instance: object,
        actor_name: str,
        actor_id: str,
    ) -> None:
        self.transport = transport
        self.instance = instance
        self.actor_name = actor_name
        self.actor_id = actor_id

    def ensure_active(self) -> None:
        pass

    def broadcast(
        self,
        message: Any,
        except_ids: tuple[str, ...] = (),
        tags: tuple[str, ...] = (),
        tag_match: str = "all",
    ) -> None:
        self.transport.broadcast(self.actor_name, self.actor_id, message)


class InMemoryActorTransport(ActorTransport):
    """In-memory RPC and broadcast transport for local testing without a live daemon."""

    def __init__(self) -> None:
        self._actors: dict[tuple[str, str], Actor[Any, Any, Any, Any]] = {}
        self._classes: dict[str, type[Actor[Any, Any, Any, Any]]] = {}
        self._broadcasts: list[dict[str, Any]] = []

    def register(self, actor_name: str, actor_cls: type[Actor[Any, Any, Any, Any]]) -> None:
        """Register an actor class by name."""
        self._classes[actor_name] = actor_cls

    def invoke(self, actor_name: str, actor_id: str, method: str, args: list[Any]) -> Any:
        """Invoke a method on the in-memory actor instance."""
        actor = self._get_or_create(actor_name, actor_id)
        func = getattr(actor, method)
        scope = _InMemoryMockScope(self, actor, actor_name, actor_id)
        token = scope_context.set(cast(Any, scope))
        try:
            return func(*args)
        finally:
            scope_context.reset(token)

    def broadcast(self, actor_name: str, actor_id: str, message: Any) -> None:
        """Record an outgoing broadcast message."""
        self._broadcasts.append(
            {"actor_name": actor_name, "actor_id": actor_id, "message": message}
        )

    def prepare_websocket(
        self,
        actor_name: str,
        actor_id: str,
        metadata: Any,
        *,
        home_region: str | None = None,
    ) -> SocketGrant:
        """Return a simulated WebSocket grant."""
        return SocketGrant(
            websocket_url=f"ws://127.0.0.1:7100/v1/actors/{actor_name}/{actor_id}/websocket",
            home_region=home_region or "local",
            connect_by_ms=int(time.time() * 1000) + 60000,
        )

    def get_broadcasts(
        self, actor_name: str | None = None, actor_id: str | None = None
    ) -> list[dict[str, Any]]:
        """Return recorded broadcasts matching optional filters."""
        return [
            b
            for b in self._broadcasts
            if (actor_name is None or b["actor_name"] == actor_name)
            and (actor_id is None or b["actor_id"] == actor_id)
        ]

    def clear_broadcasts(self) -> None:
        """Clear all recorded broadcasts."""
        self._broadcasts.clear()

    def list_actors(self, actor_name: str | None = None) -> list[str]:
        """Return all created actor IDs, optionally filtered by actor_name."""
        return [
            aid for (aname, aid) in self._actors.keys() if actor_name is None or aname == actor_name
        ]

    def as_async(self) -> AsyncActorTransport:
        """Return an async transport adapter delegating to this in-memory transport."""
        return _InMemoryAsyncAdapter(self)

    def _get_or_create(self, actor_name: str, actor_id: str) -> Actor[Any, Any, Any, Any]:
        key = (actor_name, actor_id)
        if key not in self._actors:
            cls = self._resolve_class(actor_name)
            self._actors[key] = cls()
        return self._actors[key]

    def _resolve_class(self, actor_name: str) -> type[Actor[Any, Any, Any, Any]]:
        if actor_name in self._classes:
            return self._classes[actor_name]
        if actor_name == "LangGraphCheckpointActor":
            from .langgraph import LangGraphCheckpointActor

            return LangGraphCheckpointActor
        if actor_name == "ChatMessageActor":
            from .langchain import ChatMessageActor

            return ChatMessageActor
        raise KeyError(f"Actor class {actor_name!r} not registered with InMemoryActorTransport")


class _InMemoryAsyncAdapter(AsyncActorTransport):
    def __init__(self, sync_transport: InMemoryActorTransport) -> None:
        self._sync = sync_transport

    async def invoke(self, actor_name: str, actor_id: str, method: str, args: list[Any]) -> Any:
        res = self._sync.invoke(actor_name, actor_id, method, args)
        if inspect.isawaitable(res):
            return await res
        return res

    async def broadcast(self, actor_name: str, actor_id: str, message: Any) -> None:
        self._sync.broadcast(actor_name, actor_id, message)

    def list_actors(self, actor_name: str) -> list[str]:
        return self._sync.list_actors(actor_name)

    async def prepare_websocket(
        self,
        actor_name: str,
        actor_id: str,
        metadata: Any,
        *,
        home_region: str | None = None,
    ) -> SocketGrant:
        return self._sync.prepare_websocket(actor_name, actor_id, metadata, home_region=home_region)
