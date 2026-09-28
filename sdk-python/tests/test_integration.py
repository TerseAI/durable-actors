import importlib
import os
import socket
import subprocess
import sys
import time
from contextlib import contextmanager
from queue import Queue

import httpx
import pytest

from little_actors.client import Client
from little_actors.codegen import generate_client


@pytest.mark.skipif(
    not os.environ.get("LITTLE_ACTORS_TEST_RUNTIME"), reason="requires built Rust runtime"
)
def test_python_actor_generated_client_and_durable_restart(tmp_path):
    (tmp_path / "actors.py").write_text("""from pydantic import BaseModel
from little_actors import Actor
class Count(BaseModel):
    value: int
class Counter(Actor):
    'A durable counter.'
    count: int = 0
    def increment(self, amount: int = 1) -> Count:
        'Increment the count and return its new value.'
        self.count += amount
        return Count(value=self.count)
""")
    port = free_port()
    for expected in (2, 4):
        with actor_server(tmp_path, "actors.py", port) as (client, contract):
            generate_client(contract, tmp_path / "remote")
            result = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    "from remote import actors; "
                    'assert actors.Counter.__doc__ == "A durable counter."; '
                    'assert actors.Counter.Stub.increment.__doc__ == "Increment the count and return its new value."; '
                    'print(actors.Counter.get("one").increment(2).value)',
                ],
                cwd=tmp_path,
                env={**os.environ, "DURABLE_ACTORS_CONTROL_PLANE_URL": client.origin},
                capture_output=True,
                text=True,
                timeout=15,
            )
            assert result.returncode == 0, result.stdout + result.stderr
            assert int(result.stdout) == expected


@pytest.mark.skipif(
    not os.environ.get("LITTLE_ACTORS_TEST_RUNTIME"), reason="requires built Rust runtime"
)
def test_python_generated_socket_and_state_events(tmp_path, monkeypatch):
    from little_actors import StateSnapshot, StateUpdate

    (tmp_path / "socket_actors.py").write_text("""from pydantic import BaseModel
from little_actors import Actor, ActorSocket, emitted
class Payload(BaseModel):
    value: int
class Room(Actor[Payload, Payload, Payload]):
    count: int = emitted(0)
    def on_connect(self, socket: ActorSocket[Payload, Payload]) -> None:
        socket.set_tags("connected")
    def on_message(self, socket: ActorSocket[Payload, Payload], message: Payload) -> None:
        self.count += message.value
        socket.send(Payload(value=self.count + socket.metadata.value))
    def count_connections(self) -> int:
        return len(self.get_connections())
""")
    with actor_server(tmp_path, "socket_actors.py", free_port()) as (client, contract):
        generate_client(contract, tmp_path / "socket_client")
        monkeypatch.syspath_prepend(str(tmp_path))
        remote = importlib.import_module("socket_client")
        models = remote.actors.Room
        room = remote.actors.Room.get("lobby", client)
        with room.connect(models.Payload(value=2)) as connection:
            initial = connection.receive(timeout=5)
            assert isinstance(initial, StateSnapshot)
            assert initial.state.count == 0
            assert room.count_connections() == 1
            connection.send(models.Payload(value=3))
            message = connection.receive(timeout=5)
            assert isinstance(message, models.Payload)
            assert message.value == 5
            update = connection.receive(timeout=5)
            assert isinstance(update, StateUpdate)
            assert update.changes.count == 3
            with pytest.raises(TimeoutError):
                connection.receive(timeout=0.01)


@pytest.mark.skipif(
    not os.environ.get("LITTLE_ACTORS_TEST_RUNTIME"), reason="requires built Rust runtime"
)
def test_generated_subscription_delivers_state_while_calling_rpcs(tmp_path, monkeypatch):
    (tmp_path / "actors.py").write_text("""from little_actors import Actor, emitted
class Counter(Actor):
    count: int = emitted(0)
    label: str = emitted("ready")
    def increment(self) -> int:
        self.count += 1
        return self.count
""")
    with actor_server(tmp_path, "actors.py", free_port()) as (client, contract):
        generate_client(contract, tmp_path / "subscription_client")
        monkeypatch.syspath_prepend(str(tmp_path))
        remote = importlib.import_module("subscription_client")
        counter = remote.actors.Counter.get("one", client)
        states = Queue()
        subscription = counter.subscribe(states.put)
        try:
            initial = states.get(timeout=5)
            assert (initial.count, initial.label) == (0, "ready")
            assert counter.increment() == 1
            updated = states.get(timeout=5)
            assert (updated.count, updated.label) == (1, "ready")
        finally:
            subscription.close()
        assert subscription.closed
        assert subscription.error is None


@contextmanager
def actor_server(project, entrypoint, port):
    with (project / "runtime.log").open("w+") as log:
        process = subprocess.Popen(
            [
                os.environ["LITTLE_ACTORS_TEST_RUNTIME"],
                "dev",
                "--project",
                str(project),
                "--entrypoint",
                entrypoint,
                "--port",
                str(port),
            ],
            env={**os.environ, "DURABLE_ACTORS_PYTHON": sys.executable},
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        try:
            with httpx.Client(timeout=5) as http:
                with Client(f"http://127.0.0.1:{port}", http=http) as client:
                    yield client, wait_for_contract(client, process, log)
        finally:
            if process.poll() is None:
                process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
                raise


def wait_for_contract(client, process, log):
    deadline = time.monotonic() + 30
    while process.poll() is None and time.monotonic() < deadline:
        try:
            return client.get_contract()
        except (httpx.TransportError, httpx.HTTPStatusError):
            time.sleep(0.1)
    log.seek(0)
    pytest.fail("runtime did not become ready: " + log.read())


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]
