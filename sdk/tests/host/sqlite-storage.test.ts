import assert from "node:assert/strict"
import { test } from "node:test"

import { SqliteActorDatabase } from "../../src/host/sqlite.js"
import { SqliteRecovery } from "../fixtures/sqlite.js"

test("SQLite capture returns committed WAL pages", context => {
    const database = new SqliteActorDatabase()
    context.after(() => database.close())
    database.exec("CREATE TABLE entries (value BLOB)")
    database.exec("INSERT INTO entries VALUES (zeroblob(?))", 2 * 1024 * 1024)
    const captured = database.snapshot()
    assert.equal(typeof captured, "object")
    assert.equal(typeof Reflect.get(Object(captured), "wal"), "object")
})

test("SQLite snapshots retain blobs, bound parameters, and schema metadata", context => {
    const database = new SqliteActorDatabase()
    const restored = new SqliteActorDatabase()
    const recovery = new SqliteRecovery()
    context.after(() => recovery.close())
    context.after(() => {
        database.close()
        restored.close()
    })
    database.exec("CREATE TABLE entries (name TEXT, data BLOB)")
    database.exec("INSERT INTO entries VALUES (?, ?)", "'); DROP TABLE entries; --", new Uint8Array([0, 128, 255]))
    database.exec("PRAGMA user_version = 2")
    const image = database.snapshot()
    restored.restore(recovery.apply(image))
    const [row] = restored.exec<{ name: string; data: Uint8Array }>("SELECT name, data FROM entries")
    assert.equal(row!.name, "'); DROP TABLE entries; --")
    assert.deepEqual([...row!.data], [0, 128, 255])
    assert.equal(restored.exec<{ user_version: number }>("PRAGMA user_version")[0]!.user_version, 2)
    assert.deepEqual(restored.snapshot(), { txid: image!.txid })
    database.exec("DELETE FROM entries")
    const deleted = database.snapshot()
    assert.notEqual(deleted, image)
    restored.restore(recovery.apply(deleted))
    assert.deepEqual(restored.exec("SELECT * FROM entries"), [])
})

test("rollback discards pending SQL and retains the previous snapshot", context => {
    const database = new SqliteActorDatabase()
    context.after(() => database.close())
    database.exec("CREATE TABLE entries (value TEXT)")
    const image = database.snapshot()
    database.exec("INSERT INTO entries VALUES (?)", "discarded")
    database.rollback()
    assert.deepEqual(database.exec("SELECT * FROM entries"), [])
    assert.deepEqual(database.snapshot(), { txid: image!.txid })
})

test("the runtime owns transaction and persistence configuration", context => {
    const database = new SqliteActorDatabase()
    context.after(() => database.close())
    for (const sql of [
        "BEGIN",
        "COMMIT",
        "END",
        "ROLLBACK",
        "SAVEPOINT x",
        "RELEASE x",
        "ATTACH ':memory:' AS other",
        "PRAGMA journal_mode = WAL",
        "/* comment */ COMMIT",
        "-- comment\nCOMMIT"
    ])
        assert.throws(() => database.exec(sql), /runtime|invocations/)
    assert.deepEqual(
        database.exec<{ value: number }>("SELECT 1 AS value").map(row => row.value),
        [1]
    )
    assert.equal(database.snapshot(), undefined)
})

test("malformed SQLite recovery state is rejected", () => {
    const database = new SqliteActorDatabase()
    assert.throws(() => database.restore({ txid: 1 }), /invalid actor SQLite recovery state/)
    assert.throws(() => database.restore({ txid: 0, path: "/unused" }), /invalid actor SQLite recovery state/)
    database.close()
})

test("durable acknowledgement checkpoints the WAL before the next change", context => {
    const database = new SqliteActorDatabase()
    context.after(() => database.close())
    database.exec("CREATE TABLE entries (value BLOB)")
    database.exec("CREATE TABLE counter (count INTEGER)")
    database.exec("INSERT INTO counter VALUES (0)")
    database.exec("INSERT INTO entries VALUES (zeroblob(?))", 2 * 1024 * 1024)
    const first = database.snapshot()!
    database.checkpoint(first.txid - 1)
    database.exec("UPDATE counter SET count = 1")
    const second = database.snapshot()!
    assert.equal(second.wal!.base_txid, 0)
    database.checkpoint(second.txid)
    database.exec("UPDATE counter SET count = 2")
    const third = database.snapshot()!
    assert.equal(third.wal!.base_txid, second.txid)
    assert.equal(third.txid, second.txid + 1)
    assert.ok(third.wal!.data.length < first.wal!.data.length / 10)
    assert.deepEqual(database.snapshot(), { txid: third.txid })
})

test("durable acknowledgement cannot checkpoint an overlapping SQL transaction", context => {
    const database = new SqliteActorDatabase()
    context.after(() => database.close())
    database.exec("CREATE TABLE entries (value INTEGER)")
    const first = database.snapshot()!
    database.exec("INSERT INTO entries VALUES (1)")
    database.checkpoint(first.txid)
    const second = database.snapshot()!
    assert.equal(second.txid, first.txid + 1)
    assert.equal(second.wal!.base_txid, 0)
})
