import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path

import pytest

from little_actors.build import build_actor


@pytest.mark.parametrize("generic", [False, True])
async def test_artifact_attaches_invokes_and_rehydrates_in_a_fresh_python_process(
    tmp_path, generic
):
    project = tmp_path / "project"
    project.mkdir()
    (project / "actors.py").write_text("""from little_actors import Actor
class Counter(Actor):
    count: int = 0
    async def increment(self, amount: int = 1) -> int:
        self.count += amount
        return self.count
""")
    contract = build_actor(project, "actors.py", tmp_path / "build")
    assert contract["actors"][0]["actorName"] == "Counter"
    actor = {"project_id": "local", "actor_name": "Counter", "actor_id": "one"}
    state = None
    with tempfile.TemporaryDirectory(dir="/tmp") as short:
        for expected in (2, 4):
            done = asyncio.get_running_loop().create_future()

            async def accepted(reader, writer):
                try:
                    if generic:
                        assert json.loads(await reader.readline()) == {
                            "type": "warm",
                            "protocol": 18,
                        }
                        writer.write(
                            (
                                json.dumps(
                                    {
                                        "type": "load",
                                        "entrypoint": str(tmp_path / "build/actors.pyz"),
                                    }
                                )
                                + "\n"
                            ).encode()
                        )
                        await writer.drain()
                    attached = json.loads(await reader.readline())
                    assert attached == {
                        "type": "attach",
                        "protocol": 18,
                        "actor_names": ["Counter"],
                    }
                    writer.write(b'{"type":"attached","protocol":18}\n')
                    writer.write(
                        (
                            json.dumps(
                                {
                                    "type": "command",
                                    "message_id": 1,
                                    "command": {
                                        "type": "invoke",
                                        "request_id": "r1",
                                        "actor": actor,
                                        "state": state,
                                        "method": "increment",
                                        "args": [2],
                                    },
                                }
                            )
                            + "\n"
                        ).encode()
                    )
                    await writer.drain()
                    reply = json.loads(await reader.readline())
                    assert reply["reply"]["result"] == expected
                    done.set_result(reply["reply"]["state"])
                except BaseException as error:
                    done.set_exception(error)
                finally:
                    writer.close()

            socket = str(Path(short) / "executor.sock")
            server = await asyncio.start_unix_server(accepted, socket)
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                "-m",
                "little_actors.host",
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
                state = await asyncio.wait_for(done, 10)
            finally:
                if process.returncode is None:
                    process.terminate()
                await process.communicate()
                server.close()
                await server.wait_closed()


async def test_eviction_cancels_socket_output_and_accepts_late_acknowledgments(tmp_path):
    project = tmp_path / "project"
    project.mkdir()
    (project / "eviction_actors.py").write_text("""import asyncio
from little_actors import Actor, reentrant
class Counter(Actor):
    count: int = 0
    @reentrant
    async def hold(self) -> None:
        self.broadcast("started")
        await asyncio.Event().wait()
    async def increment(self) -> int:
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
            "little_actors.host",
            env={
                **os.environ,
                "DURABLE_ACTORS_EXECUTOR_SOCKET": socket,
                "DURABLE_ACTORS_ENTRYPOINT": str(tmp_path / "build/actors.pyz"),
            },
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            async with asyncio.timeout(5):
                reader, writer = await connected

                async def receive():
                    return json.loads(await reader.readline())

                def send(message):
                    writer.write((json.dumps(message) + "\n").encode())

                assert (await receive())["type"] == "attach"
                send({"type": "attached", "protocol": 18})
                actor = {"project_id": "local", "actor_name": "Counter", "actor_id": "one"}
                invocation = {
                    "type": "invoke",
                    "request_id": "r1",
                    "actor": actor,
                    "state": None,
                    "method": "hold",
                    "args": [],
                }
                send({"type": "command", "message_id": 1, "command": invocation})
                assert (await receive())["type"] == "ready_for_invocation"
                assert (await receive())["type"] == "socket_effects"
                send(
                    {
                        "type": "command",
                        "message_id": 2,
                        "command": {"type": "evict", "actor": actor},
                    }
                )
                replies = [await receive(), await receive()]
                outcomes = {reply["message_id"]: reply["reply"] for reply in replies}
                assert outcomes[1]["code"] == "actor_evicted"
                assert outcomes[2] == {"type": "evicted"}
                send({"type": "socket_effects_published", "message_id": 1})
                send(
                    {
                        "type": "command",
                        "message_id": 3,
                        "command": {
                            **invocation,
                            "method": "increment",
                            "state": {"count": 10},
                        },
                    }
                )
                assert (await receive())["reply"]["result"] == 11
        finally:
            if process.returncode is None:
                process.terminate()
            await asyncio.wait_for(process.communicate(), 2)
            server.close()
            if connected.done():
                _, writer = connected.result()
                writer.transport.abort()
                await asyncio.wait_for(writer.wait_closed(), 1)
            await server.wait_closed()
