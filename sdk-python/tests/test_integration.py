import asyncio
import importlib
import os
import sys

import httpx
import pytest

from little_actors.client import Client
from little_actors.codegen import generate_client


@pytest.mark.skipif(
    not os.environ.get("LITTLE_ACTORS_TEST_RUNTIME"), reason="requires built Rust runtime"
)
async def test_python_actor_generated_client_and_durable_restart(tmp_path, monkeypatch):
    (tmp_path / "actors.py").write_text("""from pydantic import BaseModel
from little_actors import Actor
class Count(BaseModel):
    value: int
class Counter(Actor):
    count: int = 0
    async def increment(self, amount: int = 1) -> Count:
        self.count += amount
        return Count(value=self.count)
""")
    import socket

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    origin = f"http://127.0.0.1:{port}"
    for expected in (2, 4):
        process = await asyncio.create_subprocess_exec(
            os.environ["LITTLE_ACTORS_TEST_RUNTIME"],
            "dev",
            "--project",
            str(tmp_path),
            "--entrypoint",
            "actors.py",
            "--port",
            str(port),
            env={**os.environ, "DURABLE_ACTORS_PYTHON": sys.executable},
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            async with Client(origin) as client:
                async with asyncio.timeout(30):
                    while True:
                        if process.returncode is not None:
                            output, errors = await process.communicate()
                            pytest.fail(output.decode() + errors.decode())
                        try:
                            contract = await client.get_contract()
                            break
                        except (httpx.TransportError, httpx.HTTPStatusError):
                            await asyncio.sleep(0.1)
                generate_client(contract, tmp_path / "remote")
                monkeypatch.syspath_prepend(str(tmp_path))
                remote = importlib.import_module("remote")
                result = await remote.Counter("one", client).increment(2)
                assert result.value == expected
        finally:
            if process.returncode is None:
                process.terminate()
            async with asyncio.timeout(15):
                await process.communicate()


@pytest.mark.skipif(
    not os.environ.get("LITTLE_ACTORS_TEST_RUNTIME"), reason="requires built Rust runtime"
)
async def test_python_generated_socket_and_state_events(tmp_path, monkeypatch):
    import socket

    from little_actors import StateSnapshot, StateUpdate

    (tmp_path / "socket_actors.py").write_text("""from pydantic import BaseModel
from little_actors import Actor, ActorSocket, emitted
class Payload(BaseModel):
    value: int
class Room(Actor[Payload, Payload, Payload]):
    count: int = emitted(0)
    async def on_connect(self, socket: ActorSocket[Payload, Payload]) -> None:
        socket.set_tags("connected")
    async def on_message(self, socket: ActorSocket[Payload, Payload], message: Payload) -> None:
        self.count += message.value
        socket.send(Payload(value=self.count + socket.metadata.value))
    async def count_connections(self) -> int:
        return len(await self.get_connections())
""")
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    process = await asyncio.create_subprocess_exec(
        os.environ["LITTLE_ACTORS_TEST_RUNTIME"],
        "dev",
        "--project",
        str(tmp_path),
        "--entrypoint",
        "socket_actors.py",
        "--port",
        str(port),
        env={**os.environ, "DURABLE_ACTORS_PYTHON": sys.executable},
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        async with asyncio.timeout(30), Client(f"http://127.0.0.1:{port}") as client:
            while True:
                if process.returncode is not None:
                    output, errors = await process.communicate()
                    pytest.fail(output.decode() + errors.decode())
                try:
                    contract = await client.get_contract()
                    break
                except (httpx.TransportError, httpx.HTTPStatusError):
                    await asyncio.sleep(0.1)
            generate_client(contract, tmp_path / "socket_client")
            monkeypatch.syspath_prepend(str(tmp_path))
            remote = importlib.import_module("socket_client")
            models = importlib.import_module("socket_client.room_models")
            room = remote.Room("lobby", client)
            async with await room.connect(models.Payload(value=2)) as connection:
                initial = await connection.receive()
                assert isinstance(initial, StateSnapshot)
                assert initial.state.count == 0
                assert await room.count_connections() == 1
                await connection.send(models.Payload(value=3))
                message = await connection.receive()
                assert isinstance(message, models.Payload)
                assert message.value == 5
                update = await connection.receive()
                assert isinstance(update, StateUpdate)
                assert update.changes.count == 3
    finally:
        if process.returncode is None:
            process.terminate()
        async with asyncio.timeout(15):
            await process.communicate()
