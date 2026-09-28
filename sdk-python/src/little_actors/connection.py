from __future__ import annotations

import json
from types import TracebackType
from typing import Generic, Literal, Protocol, TypeVar

from pydantic import BaseModel, TypeAdapter
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosedOK

from .client import ActorProtocolError, SocketGrant
from .contract import encode
from .guards import is_document

Send = TypeVar("Send")
Receive = TypeVar("Receive")
State = TypeVar("State")
Patch = TypeVar("Patch")


class StateSnapshot(BaseModel, Generic[State]):
    type: Literal["state"] = "state"
    state: State
    version: int


class StateUpdate(BaseModel, Generic[Patch]):
    type: Literal["state_update"] = "state_update"
    changes: Patch
    removed: list[str]
    version: int


class SocketWire(Protocol):
    async def send(self, message: str) -> None: ...
    async def recv(self) -> str | bytes: ...
    async def close(self, code: int = 1000, reason: str = "") -> None: ...


class Connection(Generic[Send, Receive, State, Patch]):
    def __init__(
        self,
        wire: SocketWire,
        incoming: TypeAdapter[Send],
        outgoing: TypeAdapter[Receive],
        state: TypeAdapter[State],
        patch: TypeAdapter[Patch],
    ) -> None:
        self._wire = wire
        self._incoming = incoming
        self._outgoing = outgoing
        self._state = state
        self._patch = patch

    @classmethod
    async def open(
        cls,
        grant: SocketGrant,
        incoming: TypeAdapter[Send],
        outgoing: TypeAdapter[Receive],
        state: TypeAdapter[State],
        patch: TypeAdapter[Patch],
    ) -> Connection[Send, Receive, State, Patch]:
        wire = await connect(grant.websocket_url, max_size=16 * 1024 * 1024)
        return cls(wire, incoming, outgoing, state, patch)

    async def send(self, message: Send) -> None:
        data = encode(self._incoming, message)
        await self._wire.send(json.dumps(data, separators=(",", ":"), allow_nan=False))

    async def receive(self) -> Receive | StateSnapshot[State] | StateUpdate[Patch]:
        data = await self._wire.recv()
        if not isinstance(data, str):
            raise ActorProtocolError("actor sockets require JSON text")
        value = json.loads(data)
        if is_document(value) and value.get("type") in {"state", "state_update"}:
            if type(value.get("version")) is not int or value["version"] < 0:
                raise ActorProtocolError("invalid state version")
            if value["type"] == "state":
                state = self._state.validate_json(json.dumps(value["state"]), strict=True)
                return StateSnapshot[State](state=state, version=value["version"])
            patch = self._patch.validate_json(json.dumps(value["changes"]), strict=True)
            removed = TypeAdapter(list[str]).validate_python(value["removed"], strict=True)
            return StateUpdate[Patch](changes=patch, removed=removed, version=value["version"])
        return self._outgoing.validate_json(data, strict=True)

    def __aiter__(self) -> Connection[Send, Receive, State, Patch]:
        return self

    async def __anext__(self) -> Receive | StateSnapshot[State] | StateUpdate[Patch]:
        try:
            return await self.receive()
        except ConnectionClosedOK:
            raise StopAsyncIteration from None

    async def close(self, code: int = 1000, reason: str = "") -> None:
        await self._wire.close(code, reason)

    async def __aenter__(self) -> Connection[Send, Receive, State, Patch]:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close()
