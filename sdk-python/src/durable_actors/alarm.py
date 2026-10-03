from __future__ import annotations

import time
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any
from uuid import uuid4

if TYPE_CHECKING:
    from .actor import Actor
    from .sqlite import Storage

scope: ContextVar[tuple[object, Storage] | None] = ContextVar("actor_alarm", default=None)


def store(actor: object) -> Storage:
    from .socket import current_scope

    current_scope(actor)
    active = scope.get()
    if active is None or active[0] is not actor:
        raise RuntimeError("alarms are unavailable outside an actor invocation")
    return active[1]


def get_alarm(actor: object) -> int | None:
    alarm = store(actor).alarm()
    return None if alarm is None else int(alarm["deadline"])


def set_alarm(actor: object, deadline: int) -> None:
    if type(deadline) is not int or not 0 <= deadline <= 2**53 - 1:
        raise ValueError("alarm deadline must be nonnegative Unix milliseconds")
    from .actor import Actor

    if getattr(type(actor), "on_alarm", None) is Actor.on_alarm:
        raise ValueError("set_alarm requires an on_alarm handler")
    store(actor).set_alarm({"generation": str(uuid4()), "deadline": deadline})


def delete_alarm(actor: object) -> None:
    store(actor).set_alarm(None)


def deliver_alarm(actor: Actor[Any, Any, Any, Any], generation: object) -> None:
    storage = store(actor)
    current = storage.alarm()
    if current is None or current["generation"] != generation:
        return
    if current["deadline"] > time.time_ns() // 1_000_000:
        raise RuntimeError("alarm delivery arrived before its deadline")
    from .actor import Actor

    if getattr(type(actor), "on_alarm", None) is Actor.on_alarm:
        raise RuntimeError("alarm delivery requires an on_alarm handler")
    actor.on_alarm()
    current = storage.alarm()
    if current is not None and current["generation"] == generation:
        storage.set_alarm(None)
