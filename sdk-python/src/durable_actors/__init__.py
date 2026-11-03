"""Define sync or async durable actors and use typed clients and subscriptions."""

from typing import TYPE_CHECKING, Any

from .actor import Actor as Actor
from .actor import interleave as interleave
from .async_actor import AsyncActor as AsyncActor
from .async_client import AsyncActorTransport as AsyncActorTransport
from .async_client import AsyncClient as AsyncClient
from .async_connection import AsyncConnection as AsyncConnection
from .client import ActorInvocationError as ActorInvocationError
from .client import ActorProtocolError as ActorProtocolError
from .client import ActorTransport as ActorTransport
from .client import Client as Client
from .client import SocketGrant as SocketGrant
from .compute import ComputeOptions as ComputeOptions
from .compute import ComputeRegion as ComputeRegion
from .compute import compute as compute
from .connection import StateSnapshot as StateSnapshot
from .connection import StateUpdate as StateUpdate
from .database import ActorAlarm as ActorAlarm
from .database import ActorDatabase as ActorDatabase
from .database import AsyncActorAlarm as AsyncActorAlarm
from .database import AsyncActorDatabase as AsyncActorDatabase
from .database import SqliteValue as SqliteValue
from .fields import emitted as emitted
from .fields import ephemeral as ephemeral
from .fields import persisted as persisted
from .generated import UNSET as UNSET
from .generated import Unset as Unset
from .json import JsonValue as JsonValue
from .proxy import SocketAuthorization as SocketAuthorization
from .session import ActorSession as ActorSession
from .session import ActorSessionRejectedError as ActorSessionRejectedError
from .session import ActorSessionTransport as ActorSessionTransport
from .socket import ActorSocket as ActorSocket
from .subscription import AsyncSubscription as AsyncSubscription
from .subscription import Subscription as Subscription

if TYPE_CHECKING:
    from .langchain import ChatMessageActor as ChatMessageActor
    from .langchain import (
        DurableActorChatMessageHistory as DurableActorChatMessageHistory,
    )
    from .langgraph import DurableActorSaver as DurableActorSaver
    from .langgraph import LangGraphCheckpointActor as LangGraphCheckpointActor
    from .testing import InMemoryActorTransport as InMemoryActorTransport


def __getattr__(name: str) -> Any:
    if name in {"DurableActorSaver", "LangGraphCheckpointActor"}:
        from .langgraph import DurableActorSaver, LangGraphCheckpointActor

        return {
            "DurableActorSaver": DurableActorSaver,
            "LangGraphCheckpointActor": LangGraphCheckpointActor,
        }[name]
    if name in {"DurableActorChatMessageHistory", "ChatMessageActor"}:
        from .langchain import ChatMessageActor, DurableActorChatMessageHistory

        return {
            "DurableActorChatMessageHistory": DurableActorChatMessageHistory,
            "ChatMessageActor": ChatMessageActor,
        }[name]
    if name == "InMemoryActorTransport":
        from .testing import InMemoryActorTransport

        return InMemoryActorTransport
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
