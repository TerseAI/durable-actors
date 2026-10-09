import assert from "node:assert/strict"
import { test } from "node:test"

import { Actor, registerActorClass } from "../../src/actor/actor.js"
import type { ActorDatabase } from "../../src/actor/database.js"
import { Persistence } from "../../src/actor/schema.js"
import type { InvokeCommand } from "../../src/host/protocol.js"
import { ActorRuntime } from "../fixtures/actor-runtime.js"
import { recover, seed } from "../fixtures/litestream.js"
import { assertReply } from "../fixtures/reply.js"

test("constructors and field initializers can use restored SQLite and retain a handle for methods", async t => {
    class ConstructionSqlite extends Actor {
        private database = adapterDatabase(this)
        private existingTables = this.db.exec("SELECT name FROM sqlite_schema WHERE name = 'entries'").length

        protected constructor() {
            super()
            this.db.exec("CREATE TABLE IF NOT EXISTS entries (value TEXT)")
        }

        async add() {
            this.database.exec("INSERT INTO entries VALUES ('saved')")
        }
        async read() {
            return { existingTables: this.existingTables, rows: this.database.exec("SELECT value FROM entries") }
        }
    }
    const definition = registerActorClass(ConstructionSqlite, {
        actorName: "ConstructionSqlite",
        fields: ["database", "existingTables"].map(name => ({ name, persistence: Persistence.Ephemeral }))
    })
    const runtime = new ActorRuntime(definition, () => {})
    t.after(() => runtime.close())
    const sqlite = await seed()
    const command: InvokeCommand = {
        type: "invoke",
        request_id: "construction",
        actor: { project_id: "test", actor_name: "ConstructionSqlite", actor_id: "one" },
        method: "read",
        args: [],
        sqlite
    }
    assertReply(await runtime.handle(command), { type: "invoked", result: { existingTables: 0, rows: [] } })
    const saved = await runtime.handle({ ...command, method: "add" })
    assert.equal(saved.type, "invoked")
    if (saved.type !== "invoked") return
    const restored = new ActorRuntime(definition, () => {})
    t.after(() => restored.close())
    assertReply(await restored.handle({ ...command, sqlite: await recover(sqlite, saved.sqlite) }), {
        type: "invoked",
        result: { existingTables: 1, rows: [{ value: "saved" }] }
    })
})

function adapterDatabase(actor: Actor): ActorDatabase {
    return actor.db
}

test("a throwing constructor rolls back its SQLite writes before retrying", async t => {
    let attempts = 0
    class ThrowingConstruction extends Actor {
        protected constructor() {
            super()
            this.db.exec("CREATE TABLE IF NOT EXISTS startup (value INTEGER)")
            this.db.exec("INSERT INTO startup VALUES (1)")
            if (++attempts === 1) throw new Error("construction failed")
        }
        async read() {
            return this.db.exec<{ count: number }>("SELECT COUNT(*) AS count FROM startup")[0].count
        }
    }
    const runtime = new ActorRuntime(
        registerActorClass(ThrowingConstruction, { actorName: "ThrowingConstruction", fields: [] }),
        () => {}
    )
    t.after(() => runtime.close())
    const command: InvokeCommand = {
        type: "invoke",
        request_id: "throwing",
        actor: { project_id: "test", actor_name: "ThrowingConstruction", actor_id: "one" },
        method: "read",
        args: [],
        sqlite: await seed()
    }
    assertReply(await runtime.handle(command), {
        type: "failed",
        code: "invalid_actor_state",
        message: "construction failed"
    })
    assertReply(await runtime.handle(command), { type: "invoked", result: 1 })
    assert.equal(attempts, 2)
})

test("constructor database access expires before asynchronous work and does not leak to another actor", async t => {
    let escaped!: ActorDatabase
    let deferred!: Promise<unknown>
    class UnmanagedActor extends Actor {
        constructor() {
            super()
            this.db.exec("CREATE TABLE unrelated (value INTEGER)")
        }
    }
    class ScopedConstruction extends Actor {
        protected constructor() {
            super()
            escaped = this.db
            assert.throws(() => new UnmanagedActor(), /actor database is unavailable/)
            deferred = Promise.resolve().then(() => escaped.exec("CREATE TABLE escaped (value INTEGER)"))
            void deferred.catch(() => {})
        }
        async read() {
            await assert.rejects(deferred, /outside its invocation/)
            return this.db.exec("SELECT name FROM sqlite_schema WHERE name IN ('escaped', 'unrelated')")
        }
    }
    const runtime = new ActorRuntime(
        registerActorClass(ScopedConstruction, { actorName: "ScopedConstruction", fields: [] }),
        () => {}
    )
    t.after(() => runtime.close())
    assertReply(
        await runtime.handle({
            type: "invoke",
            request_id: "scoped",
            actor: { project_id: "test", actor_name: "ScopedConstruction", actor_id: "one" },
            method: "read",
            args: [],
            sqlite: await seed()
        }),
        { type: "invoked", result: [] }
    )
    assert.throws(() => escaped.exec("SELECT 1"), /outside its invocation/)
})
