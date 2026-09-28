import assert from "node:assert/strict"
import { afterEach, test } from "node:test"

import { Actor, registerActorClass } from "../../src/actor/actor.js"
import { Persistence } from "../../src/actor/schema.js"
import { ActorRuntime } from "../../src/host/actor-runtime.js"
import type { InvokeCommand } from "../../src/host/protocol.js"

class SqliteCounter extends Actor {
    count = 0

    async initialize() {
        this.db.exec("CREATE TABLE IF NOT EXISTS entries (value TEXT NOT NULL)")
    }

    async insert(value: string) {
        this.db.exec("INSERT INTO entries VALUES (?)", value)
    }

    async increment() {
        return ++this.count
    }

    async read() {
        return { count: this.count, rows: this.db.exec("SELECT value FROM entries ORDER BY rowid") }
    }

    async migrate() {
        this.db.exec("ALTER TABLE entries ADD COLUMN enabled INTEGER NOT NULL DEFAULT 1")
    }

    async fail() {
        ++this.count
        this.db.exec("INSERT INTO entries (value) VALUES ('discarded')")
        this.db.exec("CREATE TABLE discarded (id INTEGER)")
        throw new Error("rollback both stores")
    }

    async tables() {
        return this.db.exec("SELECT name FROM sqlite_schema WHERE type = 'table' ORDER BY name")
    }
}

const definition = registerActorClass(SqliteCounter, {
    actorName: "SqliteCounter",
    fields: [{ name: "count", persistence: Persistence.Persisted }]
})

const runtimes: ActorRuntime[] = []
afterEach(() => runtimes.splice(0).forEach(runtime => runtime.close()))

function createRuntime(actor = definition): ActorRuntime {
    const runtime = new ActorRuntime(actor, () => {})
    runtimes.push(runtime)
    return runtime
}

function invoke(runtime: ActorRuntime, method: string, args: string[] = [], saved: object = {}) {
    return runtime.handle({
        type: "invoke",
        request_id: method,
        actor: { project_id: "test", actor_name: "SqliteCounter", actor_id: "one" },
        method,
        args,
        state: null,
        ...saved
    } as InvokeCommand)
}

test("SQL data and schema changes persist alongside unchanged JSON fields", async () => {
    const runtime = createRuntime()
    const initialized = await invoke(runtime, "initialize")
    assert.equal(initialized.type, "invoked")
    assert.equal(typeof Reflect.get(initialized, "sqlite"), "string")
    const inserted = await invoke(runtime, "insert", ["hello ' SQL"])
    assert.equal(inserted.type, "invoked")
    assert.deepEqual(Reflect.get(inserted, "state"), { count: 0 })
    assert.notEqual(Reflect.get(inserted, "sqlite"), Reflect.get(initialized, "sqlite"))
    const migrated = await invoke(runtime, "migrate")
    assert.notEqual(Reflect.get(migrated, "sqlite"), Reflect.get(inserted, "sqlite"))

    const restored = createRuntime()
    const read = await invoke(restored, "read", [], {
        state: Reflect.get(migrated, "state"),
        sqlite: Reflect.get(migrated, "sqlite")
    })
    assert.deepEqual(Reflect.get(read, "result"), { count: 0, rows: [{ value: "hello ' SQL" }] })
    assert.equal(Reflect.get(read, "sqlite"), Reflect.get(migrated, "sqlite"))
})

test("field-only writes retain SQLite and reads retain both snapshots", async () => {
    const runtime = createRuntime()
    const initialized = await invoke(runtime, "initialize")
    const changed = await invoke(runtime, "increment")
    assert.deepEqual(Reflect.get(changed, "state"), { count: 1 })
    assert.equal(Reflect.get(changed, "sqlite"), Reflect.get(initialized, "sqlite"))
    const read = await invoke(runtime, "read")
    assert.deepEqual(Reflect.get(read, "state"), Reflect.get(changed, "state"))
    assert.equal(Reflect.get(read, "sqlite"), Reflect.get(changed, "sqlite"))
})

test("large JSON fields and SQLite databases restore together", async () => {
    const fieldBytes = 17 * 1024 * 1024
    const databaseBytes = 25 * 1024 * 1024
    class LargeState extends Actor {
        value = ""

        async initialize() {
            this.value = "x".repeat(fieldBytes)
            this.db.exec("CREATE TABLE payload (data BLOB)")
            this.db.exec("INSERT INTO payload VALUES (zeroblob(?))", databaseBytes)
        }

        async read() {
            return {
                fieldBytes: this.value.length,
                databaseBytes: this.db.exec<{ bytes: number }>("SELECT length(data) AS bytes FROM payload")[0]!.bytes
            }
        }
    }
    const actor = registerActorClass(LargeState, {
        actorName: "LargeState",
        fields: [{ name: "value", persistence: Persistence.Persisted }]
    })
    const identity = { actor: { project_id: "test", actor_name: "LargeState", actor_id: "one" } }
    const initialized = await invoke(createRuntime(actor), "initialize", [], identity)
    assert.equal(initialized.type, "invoked")
    if (initialized.type !== "invoked") return
    const read = await invoke(createRuntime(actor), "read", [], {
        ...identity,
        state: initialized.state,
        sqlite: initialized.sqlite
    })
    assert.deepEqual(Reflect.get(read, "result"), { fieldBytes, databaseBytes })
    assert.deepEqual(Reflect.get(read, "state"), initialized.state)
    assert.equal(Reflect.get(read, "sqlite"), initialized.sqlite)
})

test("failed calls roll back object fields, SQL writes, and SQL schema changes", async () => {
    const runtime = createRuntime()
    await invoke(runtime, "initialize")
    const inserted = await invoke(runtime, "insert", ["retained"])
    assert.deepEqual(await invoke(runtime, "fail"), {
        type: "failed",
        code: "actor_method_failed",
        message: "rollback both stores"
    })
    const read = await invoke(runtime, "read")
    assert.deepEqual(Reflect.get(read, "result"), { count: 0, rows: [{ value: "retained" }] })
    assert.equal(Reflect.get(read, "sqlite"), Reflect.get(inserted, "sqlite"))
    assert.deepEqual(Reflect.get(await invoke(runtime, "tables"), "result"), [{ name: "entries" }])
})

test("socket hooks persist SQL and fields in the same completion", async () => {
    class SqliteRoom extends Actor {
        count = 0
        async onConnect() {
            this.db.exec("CREATE TABLE visits (count INTEGER)")
            this.db.exec("INSERT INTO visits VALUES (?)", ++this.count)
        }
        async read() {
            return this.db.exec("SELECT count FROM visits")
        }
    }
    const actor = registerActorClass(SqliteRoom, {
        actorName: "SqliteRoom",
        fields: [{ name: "count", persistence: Persistence.Persisted }]
    })
    const runtime = createRuntime(actor)
    const identity = { project_id: "test", actor_name: "SqliteRoom", actor_id: "one" }
    const connection = { id: "socket-1", metadata: {}, tags: [] }
    const reply = await runtime.handle({
        type: "websocket_event",
        request_id: "connect",
        actor: identity,
        state: null,
        event: { type: "connect", connection },
        connections: [connection]
    })
    assert.equal(reply.type, "websocket_handled")
    if (reply.type !== "websocket_handled") return
    assert.deepEqual(reply.state, { count: 1 })
    assert.equal(typeof reply.sqlite, "string")
    assert.deepEqual(reply.effects, [])
    const restored = createRuntime(actor)
    const read = await restored.handle({
        type: "invoke",
        request_id: "read",
        actor: identity,
        method: "read",
        args: [],
        state: reply.state,
        sqlite: reply.sqlite
    })
    assert.deepEqual(Reflect.get(read, "result"), [{ count: 1 }])
})

test("reentrant failures preserve SQL already captured by an overlapping success", async () => {
    let release!: () => void
    const gate = new Promise<void>(resolve => {
        release = resolve
    })
    class ReentrantSqlite extends Actor {
        count = 0
        async hold() {
            this.db.exec("CREATE TABLE entries (count INTEGER)")
            this.db.exec("INSERT INTO entries VALUES (?)", ++this.count)
            await gate
            throw new Error("failed")
        }
        async add() {
            this.db.exec("INSERT INTO entries VALUES (?)", ++this.count)
        }
        async read() {
            return this.db.exec("SELECT count FROM entries ORDER BY count")
        }
    }
    const runtime = createRuntime(
        registerActorClass(ReentrantSqlite, {
            actorName: "ReentrantSqlite",
            fields: [{ name: "count", persistence: Persistence.Persisted }],
            reentrantMethods: ["hold"]
        })
    )
    const saved = { actor: { project_id: "test", actor_name: "ReentrantSqlite", actor_id: "one" } }
    const holding = invoke(runtime, "hold", [], saved)
    await invoke(runtime, "add", [], saved)
    release()
    assert.equal((await holding).type, "failed")
    const read = await invoke(runtime, "read", [], saved)
    assert.deepEqual(Reflect.get(read, "result"), [{ count: 1 }, { count: 2 }])
    assert.deepEqual(Reflect.get(read, "state"), { count: 2 })
})

test("compiled object schema changes preserve SQLite while adding and removing fields", async () => {
    const runtime = createRuntime()
    await invoke(runtime, "initialize")
    const inserted = await invoke(runtime, "insert", ["retained"])
    const updated = createRuntime({
        ...definition,
        state: {
            ...definition.state,
            fields: [{ name: "count", persistence: Persistence.Ephemeral }]
        }
    })
    const read = await invoke(updated, "read", [], {
        state: { count: 8, retired: true },
        sqlite: Reflect.get(inserted, "sqlite")
    })
    assert.deepEqual(Reflect.get(read, "state"), {})
    assert.deepEqual(Reflect.get(read, "result"), { count: 0, rows: [{ value: "retained" }] })
    assert.equal(Reflect.get(read, "sqlite"), Reflect.get(inserted, "sqlite"))
})

test("database handles cannot write after their invocation completes", async () => {
    let escaped!: () => void
    let release!: () => void
    let pending!: Promise<void>
    const gate = new Promise<void>(resolve => {
        release = resolve
    })
    class DetachedSqlite extends Actor {
        async start() {
            const database = this.db
            escaped = () => {
                database.exec("CREATE TABLE escaped (id INTEGER)")
            }
            pending = gate.then(escaped)
        }
    }
    const runtime = createRuntime(registerActorClass(DetachedSqlite, { actorName: "DetachedSqlite", fields: [] }))
    const reply = await invoke(runtime, "start", [], {
        actor: { project_id: "test", actor_name: "DetachedSqlite", actor_id: "one" }
    })
    assert.equal(reply.type, "invoked")
    assert.throws(escaped, /outside its invocation/)
    release()
    await assert.rejects(pending, /outside its invocation/)
})
