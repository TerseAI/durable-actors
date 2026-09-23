#!/usr/bin/env bun
import assert from "node:assert/strict"
import { once } from "node:events"
import { closeSync, writeFileSync, writeSync } from "node:fs"
import path from "node:path"
import { pathToFileURL } from "node:url"
import { Worker } from "node:worker_threads"

const option = name => process.argv[process.argv.indexOf(name) + 1]
const project = option("--project")
const host = pathToFileURL(option("--sdk-host"))
const entrypoint = path.resolve(project, option("--entrypoint"))
const { ActorCompiler } = await import(new URL("./compiler/actor-compiler.js", host).href)
const worker = new Worker(new URL("./host/actor-worker.js", host), {
    workerData: {
        moduleUrl: pathToFileURL(entrypoint).href,
        schemas: new ActorCompiler().check(entrypoint)
    }
})
try {
    const receive = async () => (await once(worker, "message", { signal: AbortSignal.timeout(10_000) }))[0]
    const loaded = await receive()
    assert.equal(loaded.type, "ready", loaded.message)
    const reply = receive()
    worker.postMessage({
        type: "command",
        messageId: 1,
        command: {
            type: "invoke",
            request_id: "first-call",
            actor: { project_id: "default", actor_name: "Counter", actor_id: "counter" },
            method: "increment",
            args: [],
            state: null
        }
    })
    const response = await reply
    assert.equal(response.reply.type, "invoked", JSON.stringify(response))
    writeFileSync(path.join(project, "invocation.json"), JSON.stringify(response.reply.result))
} finally {
    await worker.terminate()
}
writeSync(
    3,
    JSON.stringify({
        projectId: "default",
        controlPlaneUrl: "http://127.0.0.1:7100",
        apiKey: "test-key",
        storageRegion: "local",
        pid: process.pid
    })
)
closeSync(3)
if (process.env.TEST_RUNTIME_HOLD_OPEN === "1") {
    process.stdin.resume()
    process.stdin.on("end", () => process.exit(0))
}
process.exitCode = Number(process.env.TEST_RUNTIME_EXIT_CODE ?? 0)
