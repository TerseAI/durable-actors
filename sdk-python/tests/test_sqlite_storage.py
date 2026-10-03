import sqlite3

import pytest

from durable_actors.sqlite import SqliteCaptureError, SqliteStorage


@pytest.mark.asyncio
async def test_fields_commit_before_replication_and_rollback_together(tmp_path):
    path = tmp_path / "actor.sqlite"
    connection = sqlite3.connect(path)
    connection.executescript(
        "PRAGMA journal_mode=WAL; CREATE TABLE __terse_fields(name TEXT PRIMARY KEY, value TEXT NOT NULL CHECK(json_valid(value)))"
    )
    calls = []

    async def sync(state):
        calls.append(state)
        assert connection.execute(
            "SELECT value FROM __terse_fields WHERE name='count'"
        ).fetchone() == ("7",)
        return 2

    storage = SqliteStorage(sync)
    try:
        storage.restore({"path": str(path), "socket": "/tmp/test.sock", "txid": 1})
        fields = {"count": 7, "nested": {"items": [None, {"done": True}]}}
        storage.persist_fields(fields)
        assert await storage.snapshot() == {"txid": 2}
        storage.persist_fields(fields)
        assert await storage.snapshot() == {"txid": 2}
        assert len(calls) == 1
        storage.persist_fields({"count": 99})
        storage.rollback()
        assert storage.fields() == fields
        assert await storage.snapshot() == {"txid": 2}
    finally:
        storage.close()
        connection.close()


@pytest.mark.asyncio
async def test_replication_failure_fences_later_writes(tmp_path):
    path = tmp_path / "actor.sqlite"
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE __terse_fields(name TEXT PRIMARY KEY, value TEXT)")

    async def sync(state):
        raise OSError("daemon exited")

    storage = SqliteStorage(sync)
    try:
        storage.restore({"path": str(path), "socket": "/tmp/test.sock", "txid": 1})
        storage.persist_fields({"count": 1})
        with pytest.raises(SqliteCaptureError):
            await storage.snapshot()
        with pytest.raises(SqliteCaptureError):
            storage.persist_fields({"count": 2})
    finally:
        storage.close()


@pytest.mark.asyncio
async def test_sql_savepoints_scripts_metadata_and_invocation_rollback(tmp_path):
    async def sync(state):
        return 2

    storage = SqliteStorage(sync)
    storage.restore({"path": str(tmp_path / "actor.sqlite"), "socket": "/tmp/test.sock", "txid": 1})
    try:
        storage.execute("CREATE TABLE entries(value TEXT UNIQUE); CREATE TABLE audit(value TEXT)")
        storage.exec(
            "CREATE TRIGGER record_entry AFTER INSERT ON entries BEGIN INSERT INTO audit VALUES(NEW.value); INSERT INTO audit VALUES('trigger; value'); END;"
        )
        committed = await storage.snapshot()

        def outer():
            assert storage.execute("INSERT INTO entries VALUES (?)", "outer").rows_written == 3

            def inner():
                storage.exec("INSERT INTO entries VALUES ('inner')")
                raise ValueError("discard inner")

            with pytest.raises(ValueError, match="discard inner"):
                storage.transaction_sync(inner)
            return storage.transaction_sync(
                lambda: storage.execute(
                    "INSERT INTO entries VALUES ('last'); SELECT value FROM entries WHERE value = ?",
                    "outer",
                )
            )

        result = storage.transaction_sync(outer)
        assert result.rows == [{"value": "outer"}]
        assert result.rows_written == 0
        assert (
            storage.execute("UPDATE entries SET value='missing' WHERE value='absent'").rows_written
            == 0
        )
        result = storage.execute(
            "UPDATE entries SET value='changed' WHERE value='last' RETURNING value"
        )
        assert result.rows == [{"value": "changed"}]
        assert result.rows_written == 1
        for sql in (
            "INSERT INTO entries VALUES ('partial'); COMMIT",
            "INSERT INTO entries VALUES (?); SELECT 1",
            "INSERT INTO entries VALUES ('partial'); DROP TABLE __terse_fields",
        ):
            with pytest.raises((ValueError, sqlite3.Error)):
                storage.execute(sql, "parameter")
        assert storage.exec("SELECT value FROM entries ORDER BY rowid") == [
            {"value": "outer"},
            {"value": "changed"},
        ]
        storage.rollback()
        assert storage.exec("SELECT value FROM entries") == []
        assert await storage.snapshot() == committed
    finally:
        storage.close()
