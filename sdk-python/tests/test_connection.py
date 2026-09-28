import json

from pydantic import BaseModel, TypeAdapter

from little_actors.connection import Connection, StateSnapshot


class Message(BaseModel):
    text: str


class State(BaseModel):
    count: int


class Wire:
    def __init__(self):
        self.sent = []
        self.messages = [
            json.dumps({"type": "state", "state": {"count": 1}, "version": 1}),
            '{"text":"hello"}',
        ]

    async def recv(self):
        return self.messages.pop(0)

    async def send(self, value):
        self.sent.append(value)

    async def close(self, code=1000, reason=""):
        pass


async def test_socket_connection_validates_messages_and_separates_state():
    wire = Wire()
    connection = Connection(
        wire, TypeAdapter(Message), TypeAdapter(Message), TypeAdapter(State), TypeAdapter(State)
    )
    snapshot = await connection.receive()
    assert isinstance(snapshot, StateSnapshot)
    assert snapshot.state.count == 1
    assert (await connection.receive()).text == "hello"
    await connection.send(Message(text="sent"))
    assert json.loads(wire.sent[0]) == {"text": "sent"}
