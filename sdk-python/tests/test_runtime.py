from typing import Annotated

from fixtures.effects import Effects
from pydantic import BaseModel

from little_actors import Actor, Ephemeral, Persisted
from little_actors.runtime import ActorRuntime


class Value(BaseModel):
    count: int


class Counter(Actor):
    value: Annotated[Value, Persisted()] = Value(count=0)
    calls: Annotated[int, Ephemeral()] = 0

    async def increment(self, amount: int = 1) -> Value:
        self.value.count += amount
        self.calls += 1
        return self.value

    async def fail(self) -> None:
        self.value.count = 99
        raise ValueError("failed")


ACTOR = {"project_id": "local", "actor_name": "Counter", "actor_id": "one"}


def command(method="increment", args=None, **extra):
    return {
        "type": "invoke",
        "request_id": "r1",
        "actor": ACTOR,
        "method": method,
        "args": args or [],
        "state": None,
        **extra,
    }


async def test_hydrates_validates_and_snapshots_typed_state():
    runtime = ActorRuntime(Counter, Effects())
    reply = await runtime.handle(command(args=[2], state={"value": {"count": 3}}))
    assert reply["result"] == {"count": 5}
    assert reply["state"] == {"value": {"count": 5}}
    assert (await runtime.handle(command()))["result"] == {"count": 6}


async def test_failure_rolls_back_persisted_state():
    runtime = ActorRuntime(Counter, Effects())
    await runtime.handle(command())
    failure = await runtime.handle(command("fail"))
    assert failure["type"] == "failed"
    assert (await runtime.handle(command()))["result"] == {"count": 2}


async def test_residency_and_eviction_require_explicit_hydration():
    runtime = ActorRuntime(Counter, Effects())
    assert (await runtime.handle(command(resident_only=True)))["type"] == "state_required"
    await runtime.handle(command())
    assert (await runtime.handle({"type": "evict", "actor": ACTOR}))["type"] == "evicted"
    assert (await runtime.handle(command(resident_only=True)))["type"] == "state_required"


async def test_argument_validation_precedes_actor_execution():
    runtime = ActorRuntime(Counter, Effects())
    assert (await runtime.handle(command(args=["2"])))["type"] == "failed"
    assert (await runtime.handle(command()))["result"] == {"count": 1}


async def test_reentrant_failure_does_not_erase_overlapping_success():
    import asyncio

    from little_actors import reentrant

    entered, resume = asyncio.Event(), asyncio.Event()

    class Shared(Actor):
        count: Annotated[int, Persisted()] = 0

        @reentrant
        async def wait_and_fail(self) -> None:
            entered.set()
            await resume.wait()
            raise ValueError("failed")

        async def increment(self) -> int:
            self.count += 1
            return self.count

    runtime = ActorRuntime(Shared, Effects())
    actor = {**ACTOR, "actor_name": "Shared"}
    pending = asyncio.create_task(runtime.handle(command("wait_and_fail", actor=actor)))
    await asyncio.wait_for(entered.wait(), 1)
    reply = await asyncio.wait_for(runtime.handle(command(actor=actor)), 1)
    resume.set()
    assert (await pending)["type"] == "failed"
    assert reply["result"] == 1
    assert reply["sequence"] == 1
    assert (await runtime.handle(command(actor=actor)))["result"] == 2


async def test_socket_messages_are_typed_and_emit_persisted_changes():
    from fixtures.effects import Effects

    from little_actors import ActorSocket, Emittable

    class Room(Actor[Value, Value, Value]):
        value: Annotated[Value, Persisted(), Emittable()] = Value(count=0)

        async def on_message(self, socket: ActorSocket[Value, Value], message: Value) -> None:
            self.value = message
            socket.send(Value(count=message.count + socket.metadata.count))

    effects = Effects()
    runtime = ActorRuntime(Room, effects)
    reply = await runtime.handle(
        {
            "type": "websocket_event",
            "request_id": "s1",
            "actor": {**ACTOR, "actor_name": "Room"},
            "state": None,
            "connections": [{"id": "s1", "metadata": {"count": 2}, "tags": []}],
            "event": {
                "type": "message",
                "connection_id": "s1",
                "message": {"type": "text", "data": '{"count":3}'},
            },
        }
    )
    assert reply["type"] == "websocket_handled"
    assert effects.published[0]["message"]["data"] == '{"count":5}'
    assert reply["effects"][0]["changes"] == {"value": {"count": 3}}
