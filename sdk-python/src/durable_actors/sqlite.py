from __future__ import annotations

import json
import re
import sqlite3
from collections.abc import Awaitable, Callable
from threading import RLock
from typing import Any, Protocol

from .contract import Document
from .database import SqliteValue


class SqliteCaptureError(RuntimeError):
    pass


class Storage(Protocol):
    def restore(self, state: Document | None) -> None: ...
    def fields(self) -> Document: ...
    def persist_fields(self, fields: Document) -> None: ...
    def exec(self, sql: str, *bindings: SqliteValue) -> list[dict[str, SqliteValue]]: ...
    async def snapshot(self) -> Document: ...
    def rollback(self) -> None: ...
    def close(self) -> None: ...


class SqliteStorage:
    def __init__(self, commit: Callable[[], Awaitable[int]]) -> None:
        self.commit = commit
        self.lock = RLock()
        self.connection: sqlite3.Connection | None = None
        self.seed: Document | None = None
        self.txid = 0
        self.version: tuple[int, int, int] = (0, 0, 0)
        self.failure: SqliteCaptureError | None = None

    def restore(self, state: Document | None) -> None:
        self.close()
        if (
            state is None
            or not valid_txid(state.get("txid"))
            or not isinstance(state.get("path"), str)
            or not state["path"]
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
            self._persist_fields(fields)

    def _persist_fields(self, fields: Document) -> None:
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
            with self.lock:
                database = self.open()
                version = self.change_token()
                database.commit()
            if version != self.version:
                assert self.seed is not None
                txid = await self.commit()
                if not valid_txid(txid) or txid < self.txid:
                    raise ValueError("invalid host commit position")
                self.txid = txid
                self.version = version
            return {"txid": self.txid}
        except Exception as error:
            self.failure = SqliteCaptureError("failed to replicate actor SQLite commit")
            raise self.failure from error

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

    def exec(self, sql: str, *bindings: SqliteValue) -> list[dict[str, SqliteValue]]:
        if type(sql) is not str:
            raise ValueError("SQL must be a string")
        _validate_statement(sql)
        with self.lock:
            database = self.open()
            _validate_single_statement(database, sql)
            if not database.in_transaction:
                database.execute("BEGIN IMMEDIATE")
            cursor = database.execute(sql, bindings)
            if cursor.description is None:
                return []
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]

    def change_token(self) -> tuple[int, int, int]:
        database = self.open()
        return (
            database.total_changes,
            database.execute("PRAGMA schema_version").fetchone()[0],
            database.execute("PRAGMA user_version").fetchone()[0],
        )

    def field_database(self) -> sqlite3.Connection:
        database = self.open()
        if not database.in_transaction:
            # Acquire the writer lock before reads to avoid a busy read-to-write upgrade.
            database.execute("BEGIN IMMEDIATE")
        database.execute(
            "CREATE TABLE IF NOT EXISTS __terse_fields "
            "(name TEXT PRIMARY KEY, value TEXT NOT NULL CHECK(json_valid(value)))"
        )
        return database

    def open(self) -> sqlite3.Connection:
        if self.failure is not None:
            raise self.failure
        if self.connection is None:
            if self.seed is None:
                raise ValueError("actor SQLite database has not been restored")
            # Handlers run on worker threads; commits run on the event loop.
            self.connection = sqlite3.connect(
                self.seed["path"], check_same_thread=False, isolation_level=None
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


_LEADING = re.compile(r"^(?:\s|;|--[^\n]*(?:\n|$)|/\*[\s\S]*?\*/)*")
_RESERVED_NAME = re.compile(r"(?:__terse_|_litestream_)", re.IGNORECASE)
_TRANSACTION = re.compile(
    r"^(?:BEGIN|COMMIT|END|ROLLBACK|SAVEPOINT|RELEASE|ATTACH|DETACH|VACUUM)\b",
    re.IGNORECASE,
)
_PRAGMA = re.compile(r"^PRAGMA\b", re.IGNORECASE)
_ALLOWED_PRAGMA = re.compile(
    r"^PRAGMA\s+(?:main\.)?(?:table_info|table_xinfo|index_info|index_xinfo|index_list|"
    r"foreign_key_list|foreign_key_check|integrity_check|quick_check|user_version)\b",
    re.IGNORECASE,
)
_STATEMENT_TOKENS = re.compile(
    r"--[^\n]*(?:\n|$)|/\*[\s\S]*?\*/|'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"|"
    r"`(?:``|[^`])*`|\[[^\]]*\]|;"
)


def _statement_text(sql: str) -> str:
    return _LEADING.sub("", sql).strip()


def _validate_statement(sql: str) -> None:
    statement = _statement_text(sql)
    if _RESERVED_NAME.search(statement):
        raise ValueError("SQLite runtime table names are reserved")
    if _TRANSACTION.match(statement):
        raise ValueError("actor invocations own SQLite transactions and database files")
    if _PRAGMA.match(statement) and not _ALLOWED_PRAGMA.match(statement):
        raise ValueError("this SQLite pragma is managed by the actor runtime")


def _validate_single_statement(database: sqlite3.Connection, sql: str) -> None:
    for token in _STATEMENT_TOKENS.finditer(sql):
        if token.group(0) != ";" or not _statement_text(sql[token.end() :]):
            continue
        try:
            database.execute("EXPLAIN " + sql[: token.end()])
        except sqlite3.Error as error:
            if "incomplete input" not in str(error):
                raise
            continue
        raise ValueError("actor database exec accepts one SQL statement")
