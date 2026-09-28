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
    (project / "actors.py").write_text("""from typing import Annotated
from little_actors import Actor, Persisted
class Counter(Actor):
    count: Annotated[int, Persisted()] = 0
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
