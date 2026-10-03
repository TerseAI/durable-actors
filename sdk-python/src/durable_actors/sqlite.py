from __future__ import annotations

import asyncio
import inspect
import json
import re
import sqlite3
from collections.abc import Awaitable, Callable
from threading import RLock
from typing import Any, Protocol, TypeVar

import httpx

from .contract import Document
from .database import Database, SqliteResult, SqliteValue

T = TypeVar("T")


class SqliteCaptureError(RuntimeError):
    pass


class Storage(Database, Protocol):
    def restore(self, state: Document | None) -> None: ...
    def fields(self) -> Document: ...
    def persist_fields(self, fields: Document) -> None: ...
    async def snapshot(self) -> Document: ...
    def rollback(self) -> None: ...
    def close(self) -> None: ...


async def sync_litestream(state: Document) -> int:
    transport = httpx.AsyncHTTPTransport(uds=state["socket"])
    async with httpx.AsyncClient(transport=transport, timeout=35) as client:
        response = await client.post(
            "http://localhost/sync", json={"path": state["path"], "wait": True, "timeout": 30}
        )
        response.raise_for_status()
        result = response.json()
    txid = result.get("txid")
    if (
        result.get("path") != state["path"]
        or not valid_txid(txid)
        or not valid_txid(result.get("replicated_txid"))
        or result["replicated_txid"] < txid
    ):
        raise ValueError("invalid Litestream replication acknowledgement")
    return int(txid)


class SqliteStorage:
    def __init__(self, sync: Callable[[Document], Awaitable[int]] = sync_litestream) -> None:
        self.lock = RLock()
        self.savepoint = 0
        self.sync = sync
        self.connection: sqlite3.Connection | None = None
        self.seed: Document | None = None
        self.txid = 0
        self.version: tuple[int, int, int] = (0, 0, 0)
        self.failure: SqliteCaptureError | None = None

    def exec(self, sql: str, *bindings: SqliteValue) -> list[dict[str, SqliteValue]]:
        if len(statements(sql)) != 1:
            raise ValueError("actor database exec accepts one SQL statement")
        return self.execute(sql, *bindings).rows

    def execute(self, sql: str, *bindings: SqliteValue) -> SqliteResult:
        def run() -> SqliteResult:
            sources = statements(sql)
            if not sources:
                raise ValueError("SQL code did not contain a statement")
            database = self.open()
            for source in sources[:-1]:
                validate_statement(source)
                database.execute(source).fetchall()
            validate_statement(sources[-1])
            before = database.total_changes
            cursor = database.execute(sources[-1], bindings)
            names = [column[0] for column in cursor.description or ()]
            rows = [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]
            return SqliteResult(rows, database.total_changes - before)

        return self.transaction_sync(run)

    def transaction_sync(self, operation: Callable[[], T]) -> T:
        with self.lock:
            database = self.open()
            if not database.in_transaction:
                database.execute("BEGIN")
            self.savepoint += 1
            name = f"terse_savepoint_{self.savepoint}"
            database.execute(f"SAVEPOINT {name}")
            try:
                result = operation()
                if inspect.isawaitable(result):
                    if inspect.iscoroutine(result):
                        result.close()
                    raise ValueError("SQLite transaction callbacks must be synchronous")
                database.execute(f"RELEASE {name}")
                return result
            except BaseException:
                database.execute(f"ROLLBACK TO {name}")
                database.execute(f"RELEASE {name}")
                raise

    def restore(self, state: Document | None) -> None:
        with self.lock:
            self.close()
            if (
                state is None
                or not valid_txid(state.get("txid"))
                or not all(
                    isinstance(state.get(key), str) and state[key] for key in ("path", "socket")
                )
            ):
                raise ValueError("invalid actor SQLite recovery state")
            self.seed = state
            self.txid = state["txid"]
            self.version = self.change_token()

    def fields(self) -> Document:
        with self.lock:
            return {
                name: json.loads(value)
                for name, value in self.field_database().execute(
                    "SELECT name, value FROM __terse_fields"
                )
            }

    def persist_fields(self, fields: Document) -> None:
        with self.lock:
            database = self.field_database()
            database.executemany(
                "INSERT INTO __terse_fields(name, value) VALUES (?, ?) "
                "ON CONFLICT(name) DO UPDATE SET value=excluded.value WHERE value<>excluded.value",
                [
                    (name, json.dumps(value, allow_nan=False, separators=(",", ":")))
                    for name, value in fields.items()
                ],
            )
            database.execute(
                "DELETE FROM __terse_fields WHERE name NOT IN (SELECT value FROM json_each(?))",
                (json.dumps(list(fields)),),
            )

    async def snapshot(self) -> Document:
        try:
            version = await asyncio.to_thread(self.commit)
            if version != self.version:
                assert self.seed is not None
                txid = await self.sync(self.seed)
                if not valid_txid(txid) or txid < self.txid:
                    raise ValueError("invalid Litestream transaction")
                self.txid = txid
                self.version = version
            return {"txid": self.txid}
        except Exception as error:
            self.failure = SqliteCaptureError("failed to replicate actor SQLite commit")
            raise self.failure from error

    def commit(self) -> tuple[int, int, int]:
        with self.lock:
            version = self.change_token()
            self.open().commit()
            return version

    def rollback(self) -> None:
        with self.lock:
            if self.connection is not None:
                self.connection.rollback()
                self.version = self.change_token()

    def close(self) -> None:
        with self.lock:
            if self.connection is not None:
                self.connection.close()
            self.connection = None
            self.seed = None
            self.failure = None

    def change_token(self) -> tuple[int, int, int]:
        with self.lock:
            database = self.open()
            return (
                database.total_changes,
                database.execute("PRAGMA schema_version").fetchone()[0],
                database.execute("PRAGMA user_version").fetchone()[0],
            )

    def field_database(self) -> sqlite3.Connection:
        with self.lock:
            database = self.open()
            if not database.in_transaction:
                database.execute("BEGIN")
            database.execute(
                "CREATE TABLE IF NOT EXISTS __terse_fields "
                "(name TEXT PRIMARY KEY, value TEXT NOT NULL CHECK(json_valid(value)))"
            )
            return database

    def open(self) -> sqlite3.Connection:
        with self.lock:
            if self.failure is not None:
                raise self.failure
            if self.connection is None:
                if self.seed is None:
                    raise ValueError("actor SQLite database has not been restored")
                self.connection = sqlite3.connect(
                    self.seed["path"], isolation_level=None, check_same_thread=False
                )
                for pragma in (
                    "foreign_keys=ON",
                    "journal_mode=WAL",
                    "wal_autocheckpoint=0",
                    "synchronous=FULL",
                ):
                    self.connection.execute("PRAGMA " + pragma)
                if self.connection.execute("PRAGMA quick_check").fetchone() != ("ok",):
                    raise ValueError("invalid actor SQLite database")
            return self.connection


def valid_txid(value: Any) -> bool:
    return type(value) is int and 0 < value <= 2**53 - 1


def statements(sql: str) -> list[str]:
    result: list[str] = []
    start = 0
    for index, character in enumerate(sql):
        if character == ";" and sqlite3.complete_statement(sql[start : index + 1]):
            source = sql[start : index + 1]
            if without_comments(source):
                result.append(source)
            start = index + 1
    if without_comments(sql[start:]):
        result.append(sql[start:])
    return result


def without_comments(sql: str) -> str:
    return re.sub(r"^(?:\s|;|--[^\n]*(?:\n|$)|/\*[\s\S]*?\*/)*", "", sql).strip()


def validate_statement(sql: str) -> None:
    source = without_comments(sql)
    if re.search(r"(?:__terse_|_litestream_)", source, re.IGNORECASE):
        raise ValueError("SQLite runtime table names are reserved")
    if re.match(
        r"(?:BEGIN|COMMIT|END|ROLLBACK|SAVEPOINT|RELEASE|ATTACH|DETACH|VACUUM)\b",
        source,
        re.IGNORECASE,
    ):
        raise ValueError("actor invocations own SQLite transactions and database files")
    if re.match(r"PRAGMA\b", source, re.IGNORECASE) and not re.match(
        r"PRAGMA\s+(?:main\.)?(?:table_info|table_xinfo|index_info|index_xinfo|index_list|foreign_key_list|foreign_key_check|integrity_check|quick_check|user_version)\b",
        source,
        re.IGNORECASE,
    ):
        raise ValueError("this SQLite pragma is managed by the actor runtime")
