import asyncio
import json
import os

import pytest
from fixtures.executor import build, executor, warm_executor
from fixtures.host_database import HostDatabase

from durable_actors.build import build_actor
from durable_actors.supervisor import Supervisor


async def test_warm_executor_keeps_its_assigned_environment_across_worker_restarts(tmp_path):
    entrypoint = build(
        tmp_path,
        "environment_actors",
        """import os
from durable_actors import Actor
class Settings(Actor):
    def read(self, name: str) -> str | None:
        return os.environ.get(name)
""",
    )

    async def send(message_id, command):
        host.send({"type": "command", "message_id": message_id, "command": command})
        return (await host.receive())["reply"]

    read = {
        "type": "invoke",
        "request_id": "read",
        "method": "read",
        "args": ["PAYMENTS_API_KEY"],
    }
    async with warm_executor(entrypoint, {"PAYMENTS_API_KEY": "secret"}) as host:
        assert (await send(1, read))["result"] == "secret"
        assert await send(2, {"type": "evict"}) == {"type": "evicted"}
        assert (await send(3, read))["result"] == "secret"


async def test_actors_call_other_actors_through_the_host(tmp_path):
    entrypoint = build(
        tmp_path,
        "calling_actors",
        """from durable_actors import Actor
class Document(Actor):
    def publish(self) -> list[str]:
        return Index.get("main").add("doc-1")
class Index(Actor):
    def add(self, document: str) -> list[str]:
        return [document]
""",
    )
    publish = {"type": "invoke", "request_id": "publish", "method": "publish", "args": []}
    async with warm_executor(entrypoint, {}) as host:
        async with asyncio.timeout(5):
            host.send({"type": "command", "message_id": 1, "command": publish})
            call = await host.receive()
            assert call["type"] == "actor_call"
            assert {key: call["call"][key] for key in ("actor_name", "actor_id", "method")} == {
                "actor_name": "Index",
                "actor_id": "main",
                "method": "add",
            }
            assert call["call"]["args"] == ["doc-1"]
            host.send(
                {
                    "type": "actor_result",
                    "call_id": call["call_id"],
                    "outcome": {"type": "completed", "result": ["doc-1"]},
                }
            )
            assert (await host.receive())["reply"] == {"type": "invoked", "result": ["doc-1"]}


async def test_output_reaches_the_host_tagged_with_its_invocation(tmp_path):
    entrypoint = build(
        tmp_path,
        "logging_actors",
        """import sys
import threading
from durable_actors import Actor
class Greeter(Actor):
    def greet(self, name: str) -> str:
        print(f"hello {name}")
        print("careful", file=sys.stderr)
        background = threading.Thread(target=print, args=("background",))
        background.start()
        background.join()
        return name
""",
    )
    greet = {"type": "invoke", "request_id": "greet", "method": "greet", "args": ["ada"]}
    async with warm_executor(entrypoint, {}) as host:
        async with asyncio.timeout(5):
            host.send({"type": "command", "message_id": 7, "command": greet})
            assert (await host.receive())["reply"]["result"] == "ada"
    assert host.logs == [
        {"type": "log", "message_id": 7, "level": "info", "message": "hello ada"},
        {"type": "log", "message_id": 7, "level": "error", "message": "careful"},
        {"type": "log", "message_id": None, "level": "info", "message": "background"},
    ]


async def test_executor_reports_whether_its_attached_actor_is_resident(tmp_path):
    entrypoint = build(
        tmp_path,
        "residency_actors",
        """from durable_actors import Actor
class Counter(Actor):
    def ping(self) -> None:
        return None
""",
    )
    invoke = {"type": "invoke", "request_id": "ping", "method": "ping", "args": []}
    async with warm_executor(entrypoint, {}) as host:
        async with asyncio.timeout(5):
            await host.residency(False)
            host.send({"type": "command", "message_id": 1, "command": invoke})
            assert (await host.receive())["reply"]["type"] == "invoked"
            await host.residency(True)
            host.send({"type": "command", "message_id": 2, "command": {"type": "evict"}})
            await host.residency(False)


async def test_statements_reach_the_host_database_on_the_workers_own_connection(tmp_path):
    entrypoint = build(
        tmp_path,
        "sqlite_actors",
        """from durable_actors import Actor
class Notes(Actor):
    def add(self, text: str, attachment: str) -> None:
        self.db.exec("CREATE TABLE IF NOT EXISTS notes (id INTEGER PRIMARY KEY, text, file)")
        self.db.exec("INSERT INTO notes (text, file) VALUES (?, ?)", text, attachment.encode())
    def read(self) -> list[str]:
        rows = self.db.exec("SELECT id, text, file FROM notes ORDER BY id")
        return [f"{row['id']}:{row['text']}:{len(row['file'])}" for row in rows]
    def duplicate(self) -> None:
        self.db.exec("INSERT INTO notes (id) VALUES (1)")
""",
    )

    async def invoke(message_id, method, *args):
        command = {"type": "invoke", "request_id": method, "method": method, "args": list(args)}
        host.send({"type": "command", "message_id": message_id, "command": command})
        return (await host.receive())["reply"]

    large = "\u00e9" * 200_000
    async with warm_executor(entrypoint, {}) as host:
        async with asyncio.timeout(10):
            assert (await invoke(1, "add", large, "attached"))["type"] == "invoked"
            assert (await invoke(2, "read"))["result"] == [f"1:{large}:8"]
            failed = await invoke(3, "duplicate")
            assert failed["code"] == "actor_method_failed"
            assert "UNIQUE constraint failed" in failed["message"]
            host.send({"type": "command", "message_id": 4, "command": {"type": "evict"}})
            assert (await host.receive())["reply"] == {"type": "evicted"}
            assert (await invoke(5, "read"))["result"] == [f"1:{large}:8"]


@pytest.mark.parametrize("generic", [False, True])
async def test_artifact_attaches_invokes_and_rehydrates_in_a_fresh_python_process(
    tmp_path, generic
):
    project = tmp_path / "project"
    project.mkdir()
    (project / "actors.py").write_text("""from durable_actors import Actor, persisted
class Counter(Actor):
    count: int = persisted(0)
    def increment(self, amount: int = 1) -> int:
        self.count += amount
        return self.count
""")
    contract = build_actor(project, "actors.py", tmp_path / "build")
    assert contract["actors"][0]["actorName"] == "Counter"
    actor = {"project_id": "local", "actor_name": "Counter", "actor_id": "one"}
    entrypoint = str(tmp_path / "build/actors.pyz")
    increment = {"type": "invoke", "request_id": "r1", "method": "increment", "args": [2]}
    database = HostDatabase()
    for expected in (2, 4):
        async with executor(
            database, *(["--generic"] if generic else []), DURABLE_ACTORS_ENTRYPOINT=entrypoint
        ) as (host, _):
            async with asyncio.timeout(10):
                if generic:
                    assert await host.receive() == {"type": "warm", "protocol": 34}
                    host.send({"type": "load", "entrypoint": entrypoint, "environment": {}})
                assert await host.receive() == {
                    "type": "attach",
                    "protocol": 34,
                    "actor_names": ["Counter"],
                }
                host.send({"type": "attached", "protocol": 34, "actor": actor})
                host.send({"type": "command", "message_id": 1, "command": increment})
                reply = await host.receive()
                assert reply["reply"] == {"type": "invoked", "result": expected}
                assert database.fields() == {"count": expected}


async def test_graceful_eviction_runs_on_sleep_in_the_worker(tmp_path):
    project = tmp_path / "project"
    project.mkdir()
    (project / "sleep_actors.py").write_text("""from durable_actors import Actor, persisted
class Sleeper(Actor):
    calls: int = persisted(0)
    sleeps: int = persisted(0)
    def on_sleep(self) -> None:
        self.sleeps += 1
    def read(self) -> list[int]:
        self.calls += 1
        return [self.calls, self.sleeps]
""")
    build_actor(project, "sleep_actors.py", tmp_path / "build")
    entrypoint = str(tmp_path / "build/actors.pyz")

    async with executor(HostDatabase(), DURABLE_ACTORS_ENTRYPOINT=entrypoint) as (host, _):
        async with asyncio.timeout(10):
            receive, send = host.receive, host.send
            assert (await receive())["type"] == "attach"
            actor = {"project_id": "local", "actor_name": "Sleeper", "actor_id": "one"}
            send({"type": "attached", "protocol": 34, "actor": actor})
            read = {"type": "invoke", "request_id": "r", "method": "read", "args": []}
            send({"type": "command", "message_id": 0, "command": read})
            assert (await receive())["reply"]["result"] == [1, 0]
            send(
                {
                    "type": "command",
                    "message_id": 1,
                    "command": {"type": "evict", "graceful": True},
                }
            )
            # A command sent during the eviction runs afterwards on a fresh worker.
            send({"type": "command", "message_id": 2, "command": read})
            evicted = await receive()
            assert evicted["message_id"] == 1
            assert evicted["reply"] == {"type": "evicted"}
            restored = await receive()
            assert restored["message_id"] == 2
            assert restored["reply"]["result"] == [2, 1]


class StubWorker:
    actor_names: list[str] = []

    def __init__(self):
        self.sent = []

    async def send(self, message):
        self.sent.append(message)

    async def read(self):
        await asyncio.Event().wait()

    async def close(self):
        pass


class StubWriter:
    def write(self, data):
        pass

    async def drain(self):
        pass


async def test_commands_during_deferred_replay_share_one_fresh_worker():
    workers = []
    release = asyncio.Event()

    async def create_worker(entrypoint, environment):
        worker = StubWorker()
        workers.append(worker)
        await release.wait()
        return worker

    supervisor = Supervisor(asyncio.StreamReader(), StubWriter(), create_worker)
    read = {"type": "invoke", "request_id": "r", "method": "read", "args": []}
    supervisor.evicting = 1
    supervisor.deferred.append({"type": "command", "message_id": 2, "command": read})

    replay = asyncio.create_task(supervisor.finish_eviction(1))
    while not workers:
        await asyncio.sleep(0)
    await supervisor.accept({"type": "command", "message_id": 3, "command": read})
    release.set()
    await replay

    assert len(workers) == 1
    assert [message.get("message_id") for message in workers[0].sent][1:] == [2, 3]
    await supervisor.stop_worker()


async def test_deferred_command_whose_worker_fails_to_start_is_answered():
    attempts = []
    workers = []

    async def create_worker(entrypoint, environment):
        attempts.append(entrypoint)
        if len(attempts) == 1:
            raise OSError("worker failed to start")
        worker = StubWorker()
        workers.append(worker)
        return worker

    writer = RecordingWriter()
    supervisor = Supervisor(asyncio.StreamReader(), writer, create_worker)
    read = {"type": "invoke", "request_id": "r", "method": "read", "args": []}
    supervisor.evicting = 1
    supervisor.deferred.append({"type": "command", "message_id": 2, "command": read})
    supervisor.deferred.append({"type": "command", "message_id": 3, "command": read})

    await supervisor.finish_eviction(1)

    replies = {message["message_id"]: message["reply"] for message in writer.replies()}
    assert replies[1] == {"type": "evicted"}
    assert replies[2] == {
        "type": "failed",
        "code": "actor_worker_failed",
        "message": "worker failed to start",
    }
    assert not supervisor.deferred and not supervisor.replaying
    assert [message.get("message_id") for message in workers[0].sent][1:] == [3]
    await supervisor.stop_worker()


class RecordingWriter(StubWriter):
    def __init__(self):
        self.lines = []

    def write(self, data):
        self.lines.append(json.loads(data.split(b"\n", 1)[0]))

    def replies(self):
        return [line for line in self.lines if line["type"] == "reply"]


@pytest.mark.parametrize("action", ["evict", "kill_host"])
async def test_worker_stops_blocked_handlers_on_eviction_and_host_death(tmp_path, action):
    project = tmp_path / "project"
    project.mkdir()
    (project / "eviction_actors.py").write_text("""import os
from threading import Event
from durable_actors import Actor, interleave, persisted
class Counter(Actor):
    count: int = persisted(0)
    @interleave
    def hold(self) -> None:
        self.broadcast(os.getpid())
        Event().wait()
    def increment(self) -> int:
        self.count += 1
        return self.count
""")
    build_actor(project, "eviction_actors.py", tmp_path / "build")
    child_pid = None
    async with executor(
        HostDatabase(), DURABLE_ACTORS_ENTRYPOINT=str(tmp_path / "build/actors.pyz")
    ) as (host, process):
        try:
            async with asyncio.timeout(5):
                receive, send = host.receive, host.send
                assert (await receive())["type"] == "attach"
                actor = {"project_id": "local", "actor_name": "Counter", "actor_id": "one"}
                send({"type": "attached", "protocol": 34, "actor": actor})
                invocation = {
                    "type": "invoke",
                    "request_id": "r1",
                    "method": "hold",
                    "args": [],
                }
                send(
                    {
                        "type": "command",
                        "message_id": 0,
                        "command": {**invocation, "method": "increment"},
                    }
                )
                initial = await receive()
                assert initial["reply"]["sequence"] == 1
                send({"type": "command", "message_id": 1, "command": invocation})
                assert (await receive())["type"] == "ready_for_invocation"
                effect = await receive()
                assert effect["type"] == "socket_effects"
                child_pid = json.loads(effect["effects"][0]["message"]["data"])
                if action == "kill_host":
                    process.kill()
                    await process.wait()
                    for _ in range(100):
                        try:
                            os.kill(child_pid, 0)
                        except ProcessLookupError:
                            break
                        await asyncio.sleep(0.01)
                    else:
                        pytest.fail("actor worker survived its supervisor")
                    return
                send(
                    {
                        "type": "command",
                        "message_id": 2,
                        "command": {"type": "evict"},
                    }
                )
                replies = [await receive(), await receive()]
                outcomes = {reply["message_id"]: reply["reply"] for reply in replies}
                assert outcomes[1]["code"] == "actor_evicted"
                assert outcomes[2] == {"type": "evicted"}
                send(
                    {
                        "type": "command",
                        "message_id": 3,
                        "command": {**invocation, "method": "increment"},
                    }
                )
                restored = (await receive())["reply"]
                assert restored["result"] == 2
                assert restored["sequence"] == 2
        finally:
            if child_pid is not None:
                try:
                    os.kill(child_pid, 9)
                except ProcessLookupError:
                    pass
