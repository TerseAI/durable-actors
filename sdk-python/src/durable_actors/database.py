"""Invocation-scoped SQL access."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol, TypeVar
from weakref import WeakKeyDictionary, ref

from .socket import current_scope

SqliteValue = str | int | float | bytes | None
T = TypeVar("T")


@dataclass(frozen=True)
class SqliteResult:
    """Final statement rows and written-row count, including triggers and foreign keys."""

    rows: list[dict[str, SqliteValue]]
    rows_written: int


class Database(Protocol):
    def exec(self, sql: str, *bindings: SqliteValue) -> list[dict[str, SqliteValue]]: ...
    def execute(self, sql: str, *bindings: SqliteValue) -> SqliteResult: ...
    def transaction_sync(self, operation: Callable[[], T]) -> T: ...


class ActorDatabase:
    def __init__(self, actor: object, database: Database) -> None:
        self._actor = ref(actor)
        self._database = database

    def exec(self, sql: str, *bindings: SqliteValue) -> list[dict[str, SqliteValue]]:
        """Execute one statement with positional bindings and return its rows."""
        self._check()
        return self._database.exec(sql, *bindings)

    def execute(self, sql: str, *bindings: SqliteValue) -> SqliteResult:
        """Execute a script atomically; only the final statement accepts bindings and returns results."""
        self._check()
        return self._database.execute(sql, *bindings)

    def transaction_sync(self, operation: Callable[[], T]) -> T:
        """Run synchronous SQL in a nested savepoint; actor fields are outside this savepoint."""
        self._check()
        return self._database.transaction_sync(operation)

    def _check(self) -> None:
        actor = self._actor()
        if actor is None:
            raise RuntimeError("actor database is unavailable outside its invocation")
        current_scope(actor)


_databases: WeakKeyDictionary[object, ActorDatabase] = WeakKeyDictionary()


def bind_database(actor: object, database: Database) -> None:
    _databases[actor] = ActorDatabase(actor, database)


def actor_database(actor: object) -> ActorDatabase:
    try:
        return _databases[actor]
    except KeyError:
        raise RuntimeError("actor database is unavailable during construction") from None
