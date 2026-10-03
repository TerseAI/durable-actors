from fixtures.effects import Effects
from fixtures.sqlite import seed

from durable_actors import Actor, persisted
from durable_actors.runtime import ActorRuntime


async def test_commit_output_stays_in_successful_reply_and_failure_discards_it():
    class Receipts(Actor):
        count: int = persisted(0)

        def record(self, fail: bool) -> int:
            self.count += 1
            self.get_connections()[0].send_after_commit({"id": self.count})
            self.broadcast_after_commit({"count": self.count}, tags=("watcher",))
            self.broadcast("progress")
            if fail:
                raise ValueError("rejected")
            return self.count

    effects = Effects()
    effects.connections = [{"id": "socket", "metadata": None, "tags": []}]
    runtime = ActorRuntime(Receipts, effects)
    command = {
        "type": "invoke",
        "request_id": "one",
        "actor": {"project_id": "test", "actor_name": "Receipts", "actor_id": "one"},
        "method": "record",
        "args": [False],
        "sqlite": seed(),
    }
    reply = await runtime.handle(command)
    assert reply["type"] == "invoked"
    assert reply["result"] == 1
    assert [effect["type"] for effect in reply["effects"]] == ["send", "broadcast"]
    assert [effect["message"]["data"] for effect in effects.published] == ['"progress"']
    effects.published.clear()
    assert (await runtime.handle({**command, "args": [True]}))["type"] == "failed"
    assert [effect["message"]["data"] for effect in effects.published] == ['"progress"']
    assert (await runtime.handle(command))["result"] == 2
