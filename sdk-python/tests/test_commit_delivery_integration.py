import json
import os

import pytest
from test_integration import actor_server, free_port
from websockets.sync.client import connect

from durable_actors import ActorInvocationError


@pytest.mark.skipif(
    not os.environ.get("DURABLE_ACTORS_TEST_RUNTIME"), reason="requires built Rust runtime"
)
def test_committed_receipts_replay_after_restart_without_repeating_the_event(tmp_path):
    (
        tmp_path / "receipts.py"
    ).write_text("""from durable_actors import Actor, ActorSocket, persisted
import os

class Receipts(Actor):
    records: dict[str, int] = persisted(default_factory=dict)
    pending: dict[str, int] = persisted(default_factory=dict)

    def record(self, event_id: str, fail: bool = False) -> int:
        if event_id not in self.records:
            self.records[event_id] = len(self.records) + 1
        value = self.records[event_id]
        self.pending[event_id] = value
        self.broadcast_after_commit({"type": "receipt", "id": event_id, "value": value})
        if fail:
            raise ValueError("rejected")
        return value

    def confirm(self, event_id: str) -> None:
        self.pending.pop(event_id, None)

    def read(self) -> dict[str, int]:
        return self.records

    def crash(self) -> None:
        self.records["crash"] = 999
        self.broadcast_after_commit({"type": "receipt", "id": "crash", "value": 999})
        os._exit(17)

    def on_connect(self, socket: ActorSocket) -> None:
        for event_id, value in self.pending.items():
            socket.send_after_commit({"type": "receipt", "id": event_id, "value": value})
        socket.send_after_commit({"type": "ready"})
""")
    port = free_port()
    with actor_server(tmp_path, "receipts.py", port) as (client, _):
        grant = client.prepare_websocket("Receipts", "one", None)
        with connect(grant.websocket_url) as socket:
            assert json.loads(socket.recv(timeout=5)) == {"type": "ready"}
            assert client.invoke("Receipts", "one", "record", ["first"]) == 1
            assert json.loads(socket.recv(timeout=5)) == {
                "type": "receipt",
                "id": "first",
                "value": 1,
            }
            with pytest.raises(ActorInvocationError):
                client.invoke("Receipts", "one", "record", ["failed", True])
            with pytest.raises(TimeoutError):
                socket.recv(timeout=0.1)
            assert client.invoke("Receipts", "one", "read", []) == {"first": 1}
        # A committed receipt with no connection remains in the application's outbox.
        assert client.invoke("Receipts", "one", "record", ["offline"]) == 2
        with pytest.raises(ActorInvocationError):
            client.invoke("Receipts", "one", "crash", [])
        assert client.invoke("Receipts", "one", "read", []) == {"first": 1, "offline": 2}

    with actor_server(tmp_path, "receipts.py", port) as (client, _):
        grant = client.prepare_websocket("Receipts", "one", None)
        with connect(grant.websocket_url) as socket:
            assert {json.loads(socket.recv(timeout=5))["id"] for _ in range(2)} == {
                "first",
                "offline",
            }
            assert json.loads(socket.recv(timeout=5)) == {"type": "ready"}
            assert client.invoke("Receipts", "one", "record", ["offline"]) == 2
            assert json.loads(socket.recv(timeout=5)) == {
                "type": "receipt",
                "id": "offline",
                "value": 2,
            }
            for event_id in ("first", "offline"):
                client.invoke("Receipts", "one", "confirm", [event_id])
        grant = client.prepare_websocket("Receipts", "one", None)
        with connect(grant.websocket_url) as socket:
            assert json.loads(socket.recv(timeout=5)) == {"type": "ready"}
            with pytest.raises(TimeoutError):
                socket.recv(timeout=0.1)
