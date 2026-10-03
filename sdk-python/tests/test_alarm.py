import sqlite3

import pytest

from durable_actors.sqlite import SqliteStorage


@pytest.mark.asyncio
async def test_alarm_replacement_and_rollback_share_the_actor_commit(tmp_path):
    path = tmp_path / "actor.sqlite"
    sqlite3.connect(path).close()
    position = 1

    async def sync(_):
        nonlocal position
        position += 1
        return position

    storage = SqliteStorage(sync)
    storage.restore({"path": str(path), "socket": "/tmp/test.sock", "txid": 1})
    try:
        assert storage.alarm() is None
        storage.set_alarm({"generation": "first", "deadline": 123})
        assert (await storage.snapshot())["alarm"]["deadline"] == 123
        storage.set_alarm({"generation": "second", "deadline": 456})
        storage.rollback()
        assert storage.alarm() == {"generation": "first", "deadline": 123}
        storage.set_alarm(None)
        assert "alarm" not in await storage.snapshot()
    finally:
        storage.close()
