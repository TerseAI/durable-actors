import assert from "node:assert/strict"
import path from "node:path"
import { pathToFileURL } from "node:url"

import type { ActorExecutor as Executor } from "../../src/host/actor-executor.js"
import type { JsonValue } from "../../src/json.js"

import { HostDatabase } from "./host-database.js"
import { databaseChannel, sessionPort } from "./session-port.js"

const [sdk, artifact] = process.argv.slice(2)
const host = (module: string) => import(pathToFileURL(path.join(sdk!, "dist/host", module)).href)
const { ActorExecutor } = await host("actor-executor.js")
const { loadActorEntrypoint } = await host("actor-loading.js")
assert.deepEqual(await loadActorEntrypoint(pathToFileURL(artifact!).href), ["Library"])
const database = new HostDatabase()
try {
    const executor: Executor = new ActorExecutor(databaseChannel(database))
    executor.assign({ project_id: "local", actor_name: "Library", actor_id: "library-1" }, sessionPort())
    let messageId = 0
    const invoke = async (method: string, ...args: JsonValue[]) => {
        const reply = await executor.handle(++messageId, { type: "invoke", request_id: method, method, args })
        assert.equal(reply.type, "invoked", JSON.stringify(reply))
        return reply.type === "invoked" ? reply.result : undefined
    }
    assert.equal(await invoke("add", "Ada", "Notes"), 1)
    assert.equal(await invoke("add", "Ada", "Notes"), null)
    assert.deepEqual(await invoke("list"), [{ authorId: 1, bookId: 1, title: "Notes" }])
    assert.deepEqual(
        database.query("SELECT created_at FROM __drizzle_migrations").map(row => Number(row.created_at)),
        [1700000000000]
    )
    console.log("drizzle invocation passed")
} finally {
    database.close()
}
