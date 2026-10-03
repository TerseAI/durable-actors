from fixtures.effects import Effects
from fixtures.sqlite import seed

from durable_actors import Actor, persisted
from durable_actors.runtime import ActorRuntime


class Background(Actor):
    count: int = persisted(0)

    def start(self, fail: bool = False) -> int:
        self.count = 1

        def work() -> None:
            self.count = 2
            self.broadcast("finished")
            if fail:
                setattr(self, "undeclared", True)
                self.wait_until(lambda: setattr(self, "count", 999))
                raise ValueError("background failed")

        self.wait_until(work)
        if fail:
            self.wait_until(lambda: setattr(self, "count", self.count + 10))
        return self.count

    def read(self) -> int:
        return self.count


def invocation(method, args=None, **extra):
    return {
        "type": "invoke",
        "request_id": "background-test",
        "actor": {"project_id": "local", "actor_name": "Background", "actor_id": "one"},
        "method": method,
        "args": args or [],
        **extra,
    }


async def test_background_runs_in_separate_scope_and_failure_preserves_accepted_state():
    effects = Effects()
    runtime = ActorRuntime(Background, effects)
    reply = await runtime.handle(invocation("start", [True], sqlite=seed()))
    assert reply["result"] == 1
    assert runtime.database.fields() == {"count": 1}
    assert len(reply["background_tasks"]) == 2
    failed = await runtime.handle(invocation("__background", [reply["background_tasks"][0]]))
    assert failed["type"] == "failed"
    assert len(runtime.background.pending) == 1
    assert (await runtime.handle(invocation("read")))["result"] == 1
    sibling = await runtime.handle(invocation("__background", [reply["background_tasks"][1]]))
    assert sibling["type"] == "invoked"
    assert runtime.database.fields() == {"count": 11}
    reply = await runtime.handle(invocation("start"))
    finished = await runtime.handle(invocation("__background", reply["background_tasks"]))
    assert finished["type"] == "invoked"
    assert runtime.database.fields() == {"count": 2}
