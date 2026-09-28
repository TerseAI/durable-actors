"""Define durable actors and control invocation concurrency."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
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
    """Base class for durable actors with typed RPCs and WebSocket hooks.

    Public def and async def methods become RPCs. Annotated fields persist by
    default; emitted() also broadcasts their saved changes, and ephemeral() excludes
    temporary values. Use field defaults or factories instead of a constructor.

    Generic parameters describe connection metadata, incoming application
    messages, and outgoing application messages. Each defaults to JsonValue.
    """

    def __init__(self) -> None:
        """Initialize independent field defaults; the runtime restores persisted values afterward."""
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
        """Identity of this actor, available during an active invocation or socket hook."""
        from .socket import current_scope

        return current_scope(self).actor_id

    def on_connect(self, socket: ActorSocket[Metadata, Outgoing]) -> None | Awaitable[None]:
        """Handle a new WebSocket connection before it is accepted.

        Override with def or async def to inspect metadata, set tags, send a welcome
        message, or reject with socket.reject(). The default accepts the connection.
        """
        pass

    def on_message(
        self, socket: ActorSocket[Metadata, Outgoing], message: Incoming
    ) -> None | Awaitable[None]:
        """Handle a validated incoming application message from a connected client.

        Override with def or async def to update actor state or send typed replies
        through socket. The default ignores application messages.
        """
        pass

    def on_disconnect(
        self, socket: ActorSocket[Metadata, Outgoing], code: int, reason: str, was_clean: bool
    ) -> None | Awaitable[None]:
        """Handle a closed connection; override with def or async def for cleanup.

        Args:
            socket: Connection with its last known metadata and tags.
            code: WebSocket close status code.
            reason: WebSocket close reason.
            was_clean: Whether the connection completed a clean closing handshake.
        """
        pass

    def get_connections(self) -> list[ActorSocket[Metadata, Outgoing]]:
        """Return connected sockets from a synchronous actor method or hook.

        Use the returned handles only during the current invocation.
        Async handlers use await self.aget_connections().
        """
        from .socket import current_scope

        scope = current_scope(self)
        return scope.blocking(scope.get_connections)

    async def aget_connections(self) -> list[ActorSocket[Metadata, Outgoing]]:
        """Return this actor's connected sockets with typed metadata.

        Use the returned handles only during the current invocation.
        """
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
        """Queue a typed application message for selected connections.

        Args:
            message: Value matching the actor's outgoing message type.
            except_ids: Connection IDs to exclude.
            tags: Restrict delivery to matching tags; empty selects all connections.
            tag_match: Use "all" to require every tag or "any" to require one.

        Must be called during an active actor invocation or socket hook.
        """
        from .socket import current_scope

        current_scope(self).broadcast(message, except_ids, tags, tag_match)


def reentrant(method: F) -> F:
    """Allow other invocations to enter before this method finishes.

    Works with def and async def RPCs and socket hooks. Synchronous handlers
    overlap on worker threads; asynchronous handlers interleave at awaits.
    Ordinary invocations still serialize with each other and block new entries.
    Nested method calls inherit the outer invocation's admission policy.

    Reentrant actors share a live instance. Coordinate shared mutable state
    across overlapping handlers. Failed invocations do not roll back state
    anywhere in the actor class, because that could erase another call's work.
    """
    setattr(method, "__actor_reentrant__", True)
    return method
