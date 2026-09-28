from __future__ import annotations

import asyncio
import json
from contextvars import ContextVar
from typing import Any, Generic, Protocol, TypeVar

from pydantic import TypeAdapter

from .contract import Document, decode, encode
from .guards import is_document

Metadata = TypeVar("Metadata")
Outgoing = TypeVar("Outgoing")


class Effects(Protocol):
    async def publish(self, effects: list[Document]) -> None: ...
    async def get_connections(self) -> list[Document]: ...
    def admit(self) -> None: ...


class ActorSocket(Generic[Metadata, Outgoing]):
    def __init__(self, connection: Document, scope: SocketScope, state: str = "open") -> None:
        self.id: str = connection["id"]
        self._scope = scope
        self._metadata: Metadata = decode(scope.metadata, connection["metadata"])
        self._tags = tuple(connection["tags"])
        self._state = state

    @property
    def state(self) -> str:
        return self._state

    @property
    def metadata(self) -> Metadata:
        return self._metadata

    @metadata.setter
    def metadata(self, value: Metadata) -> None:
        encoded = encode(self._scope.metadata, value)
        if len(json.dumps(encoded, separators=(",", ":")).encode()) > 64 * 1024:
            raise ValueError("socket metadata exceeds 64 KiB")
        self._scope.push(
            {
                "type": "set_metadata",
                "connection_id": self.id,
                "metadata": encoded,
            }
        )
        self._metadata = value

    @property
    def tags(self) -> tuple[str, ...]:
        return self._tags

    def send(self, message: Outgoing) -> None:
        if self._state == "closed":
            raise ValueError("cannot send on a closed socket")
        self._scope.push(
            {"type": "send", "connection_id": self.id, "message": self._scope.message(message)}
        )

    def close(self, code: int = 1000, reason: str = "") -> None:
        self._close("close", code, reason)

    def reject(self, code: int = 4003, reason: str = "connection rejected") -> None:
        if self._state != "connecting":
            raise ValueError("only a connecting socket can be rejected")
        self._close("reject", code, reason)

    def set_tags(self, *tags: str) -> None:
        checked = validate_tags(tags)
        self._scope.push({"type": "set_tags", "connection_id": self.id, "tags": checked})
        self._tags = tuple(checked)

    def _close(self, kind: str, code: int, reason: str) -> None:
        if type(code) is not int or (code != 1000 and not 3000 <= code <= 4999):
            raise ValueError("close code must be 1000 or between 3000 and 4999")
        if len(reason.encode()) > 123:
            raise ValueError("close reason exceeds 123 bytes")
        if self._state != "closed":
            self._scope.push(
                {"type": kind, "connection_id": self.id, "code": code, "reason": reason}
            )
            self._state = "closed"


class SocketScope:
    def __init__(
        self,
        instance: object,
        actor_id: str,
        types: tuple[Any, Any, Any],
        effects: Effects,
        connections: list[Document] | None,
        live: bool,
    ) -> None:
        self.instance = instance
        self.actor_id = actor_id
        self.metadata, self.incoming, self.outgoing = (TypeAdapter(hint) for hint in types)
        self.effects = effects
        self.connections = connections
        self.live = live
        self.active = True
        self.pending: list[Document] = []
        self.output: asyncio.Task[None] | None = None
        self.failure: Exception | None = None
        self.sockets: dict[str, ActorSocket[Any, Any]] = {}

    async def get_connections(self) -> list[ActorSocket[Any, Any]]:
        self.ensure_active()
        connections = (
            self.connections
            if self.connections is not None
            else await self.effects.get_connections()
        )
        return [self.socket(connection) for connection in connections]

    def socket(self, connection: Document, state: str = "open") -> ActorSocket[Any, Any]:
        if connection["id"] not in self.sockets:
            self.sockets[connection["id"]] = ActorSocket(connection, self, state)
        return self.sockets[connection["id"]]

    def broadcast(
        self, message: Any, except_ids: tuple[str, ...], tags: tuple[str, ...], tag_match: str
    ) -> None:
        if tag_match not in ("all", "any") or len(except_ids) > 128:
            raise ValueError("invalid broadcast selectors")
        self.push(
            {
                "type": "broadcast",
                "message": self.message(message),
                "except_connection_ids": list(except_ids),
                "tags": validate_tags(tags),
                "tag_match": tag_match,
            }
        )

    def message(self, value: Any) -> Document:
        encoded = encode(self.outgoing, value)
        if is_document(encoded) and encoded.get("type") in {"state", "state_update"}:
            raise ValueError("state and state_update messages are reserved")
        data = json.dumps(encoded, separators=(",", ":"), allow_nan=False)
        if len(data.encode()) > 16 * 1024 * 1024:
            raise ValueError("socket message exceeds 16 MiB")
        return {"type": "text", "data": data}

    def push(self, effect: Document) -> None:
        self.ensure_active()
        if self.failure:
            raise self.failure
        if (
            len(self.pending) >= 512
            or len(json.dumps([*self.pending, effect]).encode()) > 24 * 1024 * 1024
        ):
            raise ValueError("socket output queue is full")
        self.pending.append(effect)
        if self.live and self.output is None:
            self.output = asyncio.create_task(self.drain())

    async def finish(self) -> list[Document]:
        if self.output is not None:
            await self.output
        if self.failure:
            raise self.failure
        return list(self.pending)

    async def drain(self) -> None:
        try:
            while self.pending:
                batch, self.pending = self.pending, []
                await self.effects.publish(batch)
        except Exception as error:
            self.failure = error
        finally:
            self.output = None

    def ensure_active(self) -> None:
        if not self.active:
            raise ValueError("socket scope is no longer active")


scope_context: ContextVar[SocketScope] = ContextVar("actor_socket_scope")


def current_scope(instance: object) -> SocketScope:
    scope = scope_context.get()
    scope.ensure_active()
    if scope.instance is not instance:
        raise ValueError("actor is outside its invocation")
    return scope


def validate_tags(tags: tuple[str, ...]) -> list[str]:
    unique = list(dict.fromkeys(tags))
    if (
        len(unique) > 128
        or any(not tag or len(tag) > 256 for tag in unique)
        or sum(len(tag.encode()) for tag in unique) > 8192
    ):
        raise ValueError("invalid socket tags")
    return unique
