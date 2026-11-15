"""The connection an actor's database calls travel on, apart from its commands.

One call is answered at a time, and the thread that makes it waits for the answer. A request is
a ``u32`` size, that much JSON, then the bound values the JSON counts under ``bindings``. The
answer is ``COLUMNS`` and a ``ROW`` per row while a statement runs, then ``RESULT`` or
``FAILED``. Integers are little-endian, and a value is a tag followed by its eight bytes, or by
a ``u32`` size and that many bytes.
"""

from __future__ import annotations

import json
import socket
import struct
import threading
from typing import BinaryIO, cast

from .contract import Document
from .database import SqliteValue
from .executor_wire import encode
from .guards import is_document

NULL, INTEGER, REAL, TEXT, BLOB = range(5)
COLUMNS, ROW, RESULT, FAILED = 0x10, 0x11, 0x12, 0x13

# SQLite's own limit on the values one statement binds. The Rust host ends the channel beyond it.
MAX_BINDINGS = 32_766
MAX_SIZE = 0xFFFF_FFFF


class DatabaseChannelError(RuntimeError):
    """The channel is unusable: an exchange stopped part way, so the next would be misread."""


class DatabaseChannel:
    def __init__(self, stream: BinaryIO) -> None:
        self.stream = stream
        self.exchange = threading.Lock()
        self.failure: DatabaseChannelError | None = None

    @classmethod
    def connect(cls, path: str) -> DatabaseChannel:
        connection = socket.socket(socket.AF_UNIX)
        connection.connect(path)
        return cls(cast(BinaryIO, connection.makefile("rwb")))

    def call(self, call: Document, invocation: int | None = None) -> Document:
        """Holds the calling thread until the Rust host has answered.

        ``invocation`` names the command whose database the call reaches. Without it, the
        activation's.
        """
        # A call that cannot be sent is refused before anything is written, which leaves the
        # channel usable.
        request = encode_request(call, invocation)
        with self.exchange:
            if self.failure is not None:
                raise self.failure
            try:
                self.stream.writelines(request)
                self.stream.flush()
                answer = self.answer()
            except Exception as error:
                self.failure = DatabaseChannelError(f"actor database channel failed: {error}")
                raise self.failure from error
        if answer["type"] == "failed":
            raise RuntimeError(answer["message"])
        return answer

    # Rows written before a statement fails are dropped with it.
    def answer(self) -> Document:
        columns: list[str] = []
        rows: list[list[SqliteValue]] = []
        while True:
            record = self.take(1)[0]
            if record == COLUMNS:
                columns = [self.text() for _ in range(self.u32())]
            elif record == ROW:
                rows.append([self.value() for _ in columns])
            elif record == RESULT:
                return result(self.sized(), columns, rows)
            elif record == FAILED:
                return {"type": "failed", "message": self.text()}
            else:
                raise ValueError(f"unknown record {record}")

    def value(self) -> SqliteValue:
        kind = self.take(1)[0]
        if kind == NULL:
            return None
        if kind == INTEGER:
            return int.from_bytes(self.take(8), "little", signed=True)
        if kind == REAL:
            return float(struct.unpack("<d", self.take(8))[0])
        if kind == TEXT:
            return self.text()
        if kind == BLOB:
            return self.sized()
        raise ValueError(f"unknown value type {kind}")

    def text(self) -> str:
        return self.sized().decode(errors="replace")

    def sized(self) -> bytes:
        return self.take(self.u32())

    def u32(self) -> int:
        return int.from_bytes(self.take(4), "little")

    def take(self, size: int) -> bytes:
        data = self.stream.read(size)
        if len(data) != size:
            raise EOFError("the stream closed inside a value")
        return data


def encode_request(call: Document, invocation: int | None) -> list[bytes]:
    bindings: list[object] = call["bindings"] if call["op"] == "exec" else []
    if len(bindings) > MAX_BINDINGS:
        raise ValueError(f"a statement binds at most {MAX_BINDINGS} values, not {len(bindings)}")
    head: Document = {
        "call": {name: value for name, value in call.items() if name != "bindings"},
        "bindings": len(bindings),
    }
    if invocation is not None:
        head["message_id"] = invocation
    request = sized(encode(head))
    for position, value in enumerate(bindings, 1):
        request.extend(encode_value(value, position))
    return request


def encode_value(value: object, position: int) -> list[bytes]:
    if value is None:
        return [bytes([NULL])]
    if isinstance(value, int):
        if not -(2**63) <= value < 2**63:
            raise OverflowError(f"binding {position} does not fit a 64-bit SQLite integer")
        return [struct.pack("<Bq", INTEGER, value)]
    if isinstance(value, float):
        return [struct.pack("<Bd", REAL, value)]
    if isinstance(value, str):
        return [bytes([TEXT]), *sized(value.encode())]
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, (bytes, bytearray)):
        return [bytes([BLOB]), *sized(bytes(value))]
    raise TypeError(
        f"binding {position} is {type(value).__name__}; "
        "SQLite binds None, int, float, str and bytes"
    )


def sized(body: bytes) -> list[bytes]:
    if len(body) > MAX_SIZE:
        raise OverflowError("a database value is larger than 4 GiB")
    return [struct.pack("<I", len(body)), body]


def result(body: bytes, columns: list[str], rows: list[list[SqliteValue]]) -> Document:
    document = json.loads(body)
    if not is_document(document):
        raise ValueError("database results must be objects")
    if document.get("type") == "rows":
        return {"type": "rows", "columns": columns, "rows": rows}
    return document
