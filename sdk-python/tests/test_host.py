import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path

import pytest
from fixtures.executor import ExecutorHost, build, warm_executor
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
    database = HostDatabase()
    with tempfile.TemporaryDirectory(dir="/tmp") as short:
        for expected in (2, 4):
            done = asyncio.get_running_loop().create_future()

            async def accepted(reader, writer):
                host = ExecutorHost(reader, writer, database)
                try:
                    if generic:
                        assert await host.receive() == {"type": "warm", "protocol": 30}
                        host.send(
                            {
                                "type": "load",
                                "entrypoint": str(tmp_path / "build/actors.pyz"),
                                "environment": {},
                            }
                        )
                    assert await host.receive() == {
                        "type": "attach",
                        "protocol": 30,
                        "actor_names": ["Counter"],
                    }
                    host.send({"type": "attached", "protocol": 30, "actor": actor})
                    host.send(
                        {
                            "type": "command",
                            "message_id": 1,
                            "command": {
                                "type": "invoke",
                                "request_id": "r1",
                                "method": "increment",
                                "args": [2],
                            },
                        }
                    )
                    reply = await host.receive()
                    assert reply["reply"] == {"type": "invoked", "result": expected}
                    assert database.fields() == {"count": expected}
                    done.set_result(None)
                except BaseException as error:
                    done.set_exception(error)
                finally:
                    writer.close()

            socket = str(Path(short) / "executor.sock")
            server = await asyncio.start_unix_server(accepted, socket)
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                "-m",
                "durable_actors.host",
                *(["--generic"] if generic else []),
                env={
                    **os.environ,
                    "DURABLE_ACTORS_EXECUTOR_SOCKET": socket,
                    "DURABLE_ACTORS_ENTRYPOINT": str(tmp_path / "build/actors.pyz"),
                },
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                await asyncio.wait_for(done, 10)
            finally:
                if process.returncode is None:
                    process.terminate()
                await process.communicate()
                server.close()
                await server.wait_closed()


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
    connected = asyncio.get_running_loop().create_future()

    async def accepted(reader, writer):
        connected.set_result((reader, writer))

    with tempfile.TemporaryDirectory(dir="/tmp") as short:
        socket = str(Path(short) / "executor.sock")
        server = await asyncio.start_unix_server(accepted, socket)
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "durable_actors.host",
            env={
                **os.environ,
                "DURABLE_ACTORS_EXECUTOR_SOCKET": socket,
                "DURABLE_ACTORS_ENTRYPOINT": str(tmp_path / "build/actors.pyz"),
            },
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            async with asyncio.timeout(10):
                reader, writer = await connected
                host = ExecutorHost(reader, writer, HostDatabase())
                receive, send = host.receive, host.send
                assert (await receive())["type"] == "attach"
                actor = {"project_id": "local", "actor_name": "Sleeper", "actor_id": "one"}
                send({"type": "attached", "protocol": 30, "actor": actor})
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
        finally:
            if process.returncode is None:
                process.terminate()
            await asyncio.wait_for(process.communicate(), 5)
            server.close()
            if connected.done():
                _, writer = connected.result()
                writer.transport.abort()
                await asyncio.wait_for(writer.wait_closed(), 1)
            await server.wait_closed()


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
    connected = asyncio.get_running_loop().create_future()

    async def accepted(reader, writer):
        connected.set_result((reader, writer))

    with tempfile.TemporaryDirectory(dir="/tmp") as short:
        socket = str(Path(short) / "executor.sock")
        server = await asyncio.start_unix_server(accepted, socket)
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "durable_actors.host",
            env={
                **os.environ,
                "DURABLE_ACTORS_EXECUTOR_SOCKET": socket,
                "DURABLE_ACTORS_ENTRYPOINT": str(tmp_path / "build/actors.pyz"),
            },
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        child_pid = None
        try:
            async with asyncio.timeout(5):
                reader, writer = await connected
                host = ExecutorHost(reader, writer, HostDatabase())
                receive, send = host.receive, host.send
                assert (await receive())["type"] == "attach"
                actor = {"project_id": "local", "actor_name": "Counter", "actor_id": "one"}
                send({"type": "attached", "protocol": 30, "actor": actor})
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
            if process.returncode is None:
                process.terminate()
            await asyncio.wait_for(process.communicate(), 2)
            server.close()
            if connected.done():
                _, writer = connected.result()
                writer.transport.abort()
                await asyncio.wait_for(writer.wait_closed(), 1)
            await server.wait_closed()
