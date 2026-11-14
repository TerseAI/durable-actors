import assert from "node:assert/strict"
import path from "node:path"
import { pathToFileURL } from "node:url"

import type { SocketEffect } from "../../src/actor/socketProtocol.js"
import type { ActorExecutor as Executor } from "../../src/host/actor-executor.js"

import { HostDatabase } from "./host-database.js"
import { databaseChannel, sessionPort } from "./session-port.js"

const [sdk, artifact] = process.argv.slice(2)
const host = (module: string) => import(pathToFileURL(path.join(sdk!, "dist/host", module)).href)
const { ActorExecutor } = await host("actor-executor.js")
const { loadActorEntrypoint } = await host("actor-loading.js")
assert.deepEqual(await loadActorEntrypoint(pathToFileURL(artifact!).href), ["Counter"])
const database = new HostDatabase()
const effects: SocketEffect[] = []
const executor: Executor = new ActorExecutor(databaseChannel(database))
executor.assign(
    { project_id: "local", actor_name: "Counter", actor_id: "counter-1" },
    sessionPort({ publish: (_, published) => effects.push(...published) })
)
const reply = await executor.handle(1, { type: "invoke", request_id: "request", method: "read", args: [] })
assert.equal(reply.type, "invoked")
if (reply.type === "invoked") assert.equal(reply.result, "counter-1:0")
assert.equal(effects[0]?.type, "broadcast")
console.log("invocation passed")
