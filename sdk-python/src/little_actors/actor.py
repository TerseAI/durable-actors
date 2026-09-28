from __future__ import annotations

from collections.abc import Callable
from copy import deepcopy
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from typing_extensions import TypeVar as DefaultTypeVar

from .json import JsonValue

if TYPE_CHECKING:
    from .socket import ActorSocket

Metadata = DefaultTypeVar("Metadata", default=JsonValue)
Incoming = DefaultTypeVar("Incoming", default=JsonValue)
Outgoing = DefaultTypeVar("Outgoing", default=JsonValue)
F = TypeVar("F", bound=Callable[..., Any])


class Actor(Generic[Metadata, Incoming, Outgoing]):
    def __init__(self) -> None:
        from .contract import describe_actor

        for field in describe_actor(type(self)).fields.values():
            value = (
                field.default_factory()
                if field.default_factory is not None
                else deepcopy(field.default)
            )
            setattr(self, field.name, value)

    @property
    def id(self) -> str:
        from .socket import current_scope

        return current_scope(self).actor_id

    async def on_connect(self, socket: ActorSocket[Metadata, Outgoing]) -> None:
        pass

    async def on_message(self, socket: ActorSocket[Metadata, Outgoing], message: Incoming) -> None:
        pass

    async def on_disconnect(
        self, socket: ActorSocket[Metadata, Outgoing], code: int, reason: str, was_clean: bool
    ) -> None:
        pass

    async def get_connections(self) -> list[ActorSocket[Metadata, Outgoing]]:
        from .socket import current_scope

        return await current_scope(self).get_connections()

    def broadcast(
        self,
        message: Outgoing,
        *,
        except_ids: tuple[str, ...] = (),
        tags: tuple[str, ...] = (),
        tag_match: str = "all",
    ) -> None:
        from .socket import current_scope

        current_scope(self).broadcast(message, except_ids, tags, tag_match)


def reentrant(method: F) -> F:
    setattr(method, "__actor_reentrant__", True)
    return method
