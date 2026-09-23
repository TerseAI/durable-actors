import { Server, ServerCredentials, loadPackageDefinition } from "@grpc/grpc-js"
import { loadSync } from "@grpc/proto-loader"
import { build } from "esbuild"
import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { mkdtemp, rm } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { test } from "node:test"
import { fileURLToPath } from "node:url"
import { promisify } from "node:util"

const run = promisify(execFile)

test("bundled actor transport invokes and publishes without schema assets", { timeout: 30_000 }, async t => {
    const directory = await mkdtemp(path.join(os.tmpdir(), "actor-client-bundle-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    await build({
        entryPoints: [fileURLToPath(new URL("../../dist/client/actorHostGrpc.js", import.meta.url))],
        outfile: path.join(directory, "client.mjs"),
        bundle: true,
        platform: "node",
        format: "esm",
        banner: { js: 'import { createRequire } from "node:module"; const require = createRequire(import.meta.url);' },
        logLevel: "silent"
    })
    const calls = []
    const server = actorHost(calls)
    t.after(() => server.forceShutdown())
    const port = await listen(server)

    await run(process.execPath, ["--input-type=module", "--eval", invocation(port)], { cwd: directory, timeout: 20_000 })

    assert.deepEqual(calls, ["invoke", "publish"])
})

function actorHost(calls) {
    const definition = loadPackageDefinition(
        loadSync(fileURLToPath(new URL("../../../proto/durable_actors.proto", import.meta.url)), {
            defaults: true,
            longs: Number,
            oneofs: true
        })
    )
    const server = new Server()
    server.addService(definition.durable_actors.v1.ActorHostService.service, {
        invoke(call, callback) {
            assert.equal(call.metadata.get("authorization")[0], "Bearer bundle-token")
            assert.deepEqual(call.request, {
                invocation: {
                    requestId: "request-1",
                    actor: { projectId: "team-a", actorName: "Counter", actorId: "counter-1" },
                    method: "increment",
                    argsJson: Buffer.from("[2]")
                },
                ownerEpoch: 3
            })
            calls.push("invoke")
            callback(null, { completed: { resultJson: Buffer.from("7"), socketEffectsJson: Buffer.from("[]") } })
        },
        publishSocketEffects(call, callback) {
            assert.equal(call.metadata.get("authorization")[0], "Bearer bundle-token")
            assert.deepEqual(call.request, {
                actor: { projectId: "team-a", actorName: "Counter", actorId: "counter-1" },
                ownerEpoch: 3,
                effectsJson: Buffer.from("[]")
            })
            calls.push("publish")
            callback(null, {})
        }
    })
    return server
}

function listen(server) {
    return new Promise((resolve, reject) => {
        server.bindAsync("127.0.0.1:0", ServerCredentials.createInsecure(), (error, port) => {
            if (error) reject(error)
            else resolve(port)
        })
    })
}

function invocation(port) {
    return `
        import assert from "node:assert/strict"
        import { GrpcActorHostTransport } from "./client.mjs"
        const transport = new GrpcActorHostTransport()
        const target = { route: "http://127.0.0.1:${port}", token: "bundle-token", ownerEpoch: 3, expiresAtMs: 4_000_000_000_000 }
        const actor = { projectId: "team-a", actorName: "Counter", actorId: "counter-1" }
        const reply = await transport.invoke(target, { ...actor, requestId: "request-1", method: "increment", args: [2] })
        assert.deepEqual(reply, { type: "completed", result: 7, effects: [] })
        await transport.publish(target, actor, [])
    `
}
