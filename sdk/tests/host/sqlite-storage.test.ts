import assert from "node:assert/strict"
import { test } from "node:test"

import { SqliteActorDatabase } from "../../src/host/sqlite.js"

test("SQLite snapshots retain blobs, bound parameters, and schema metadata", context => {
    const database = new SqliteActorDatabase()
    const restored = new SqliteActorDatabase()
    context.after(() => {
        database.close()
        restored.close()
    })
    database.exec("CREATE TABLE entries (name TEXT, data BLOB)")
    database.exec("INSERT INTO entries VALUES (?, ?)", "'); DROP TABLE entries; --", new Uint8Array([0, 128, 255]))
    database.exec("PRAGMA user_version = 2")
    const image = database.snapshot()
    restored.restore(image)
    const [row] = restored.exec<{ name: string; data: Uint8Array }>("SELECT name, data FROM entries")
    assert.equal(row!.name, "'); DROP TABLE entries; --")
    assert.deepEqual([...row!.data], [0, 128, 255])
    assert.equal(restored.exec<{ user_version: number }>("PRAGMA user_version")[0]!.user_version, 2)
    assert.equal(restored.snapshot(), image)
    database.exec("DELETE FROM entries")
    const deleted = database.snapshot()
    assert.notEqual(deleted, image)
    restored.restore(deleted)
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
    assert.equal(database.snapshot(), image)
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

test("malformed SQLite recovery images are rejected", () => {
    const database = new SqliteActorDatabase()
    for (const image of ["", "not-base64", Buffer.from("not a SQLite database").toString("base64")])
        assert.throws(() => database.restore(image), /invalid actor SQLite snapshot/)
    database.close()
})
