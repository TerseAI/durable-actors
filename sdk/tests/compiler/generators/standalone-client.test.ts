import { Server, ServerCredentials, loadPackageDefinition } from "@grpc/grpc-js"
import type { ServerUnaryCall, ServiceClientConstructor, sendUnaryData } from "@grpc/grpc-js"
import { loadSync } from "@grpc/proto-loader"
import { transform } from "esbuild"
import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises"
import { createServer } from "node:http"
import { tmpdir } from "node:os"
import path from "node:path"
import { test } from "node:test"
import type { TestContext } from "node:test"
import { fileURLToPath } from "node:url"
import { promisify } from "node:util"
import ts from "typescript"

import { generateClient } from "../../../src/compiler/generators/client-generator.js"

test("generated clients typecheck and call real transports without installed packages", async t => {
    const directory = await standaloneClient(t)
    const host = await actorHost(t)
    const control = await controlPlane(t, host.port)
    await runClient(directory, control.port)
    assert.equal(host.calls(), 1)
    assert.deepEqual(control.requests, [
        "/v1/projects/example/actors/ChatRoom/lobby/find-actor",
        "/v1/projects/example/actors/ChatRoom/lobby/find-websocket"
    ])
})

async function standaloneClient(t: TestContext): Promise<string> {
    const directory = await mkdtemp(path.join(tmpdir(), "standalone-actors-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    const contract = JSON.parse(
        await readFile(new URL("../../../../tests/fixtures/public-contract.json", import.meta.url), "utf8")
    )
    const generated = path.join(directory, "generated")
    await generateClient(contract, generated)
    await writeFile(path.join(directory, "package.json"), '{"type":"module"}')
    const consumer = path.join(directory, "consumer.ts")
    await writeFile(
        consumer,
        `import { actors } from "./generated/index.js"
        const room = actors.ChatRoom.get("lobby")
        const result: Promise<{ id: string; text: string }> = room.sendMessage({ text: "hello" })
        // @ts-expect-error actor method arguments remain typed
        room.sendMessage({ text: 123 })
        actors.ChatRoom.prepareWebsocket({ actorId: "lobby", metadata: {} })`
    )
    const program = ts.createProgram([consumer], {
        strict: true,
        noEmit: true,
        target: ts.ScriptTarget.ES2022,
        module: ts.ModuleKind.NodeNext,
        typeRoots: []
    })
    assert.deepEqual(
        ts.getPreEmitDiagnostics(program).map(d => ts.flattenDiagnosticMessageText(d.messageText, "\n")),
        []
    )
    const source = await readFile(path.join(generated, "index.ts"), "utf8")
    await writeFile(path.join(generated, "index.js"), (await transform(source, { loader: "ts", format: "esm" })).code)

    return directory
}

async function actorHost(t: TestContext) {
    const host = new Server()
    t.after(() => host.forceShutdown())
    const definition = loadPackageDefinition(
        loadSync(fileURLToPath(new URL("../../../src/generated/durable_actors.proto", import.meta.url)), {
            defaults: true,
            longs: Number,
            oneofs: true
        })
    ) as unknown as { durable_actors: { v1: { ActorHostService: ServiceClientConstructor } } }
    let calls = 0
    host.addService(definition.durable_actors.v1.ActorHostService.service, {
        invoke(
            call: ServerUnaryCall<{ invocation: { actor: unknown; argsJson: Buffer }; ownerEpoch: number }, unknown>,
            callback: sendUnaryData<unknown>
        ) {
            calls++
            assert.equal(call.metadata.get("authorization")[0], "Bearer ticket")
            assert.deepEqual(call.request.invocation.actor, {
                projectId: "example",
                actorName: "ChatRoom",
                actorId: "lobby"
            })
            assert.equal(call.request.ownerEpoch, 1)
            assert.deepEqual(JSON.parse(call.request.invocation.argsJson.toString()), [{ text: "hello" }])
            callback(null, {
                completed: {
                    resultJson: Buffer.from('{"id":"one","text":"hello"}'),
                    socketEffectsJson: Buffer.from("[]")
                }
            })
        }
    })
    const hostPort = await new Promise<number>((resolve, reject) =>
        host.bindAsync("127.0.0.1:0", ServerCredentials.createInsecure(), (error, port) =>
            error ? reject(error) : resolve(port)
        )
    )
    return { port: hostPort, calls: () => calls }
}

async function controlPlane(t: TestContext, hostPort: number) {
    const requests: string[] = []
    const control = createServer((request, response) => {
        assert.equal(request.headers.authorization, "Bearer secret")
        requests.push(request.url!)
        response.setHeader("content-type", "application/json")
        response.end(
            JSON.stringify(
                request.url!.endsWith("find-actor")
                    ? {
                          route: `http://127.0.0.1:${hostPort}`,
                          token: "ticket",
                          ownerEpoch: 1,
                          expiresAtMs: Date.now() + 60000
                      }
                    : {
                          websocketUrl: "wss://example.com/socket",
                          homeRegion: "us",
                          connectByMs: 1000,
                          authorizedUntilMs: 2000
                      }
            )
        )
    })
    t.after(() => control.close())
    await new Promise<void>(resolve => control.listen(0, "127.0.0.1", resolve))
    const address = control.address() as { port: number }
    return { port: address.port, requests }
}

async function runClient(directory: string, port: number): Promise<void> {
    await writeFile(
        path.join(directory, "run.mjs"),
        `import assert from "node:assert/strict"
        import { actors } from "./generated/index.js"
        assert.deepEqual(await actors.ChatRoom.get("lobby").sendMessage({ text: "hello" }), { id: "one", text: "hello" })
        assert.equal((await actors.ChatRoom.prepareWebsocket({ actorId: "lobby", metadata: {} })).websocketUrl, "wss://example.com/socket")`
    )
    await promisify(execFile)(process.execPath, [path.join(directory, "run.mjs")], {
        cwd: directory,
        timeout: 20000,
        env: {
            ...process.env,
            NODE_PATH: "",
            DURABLE_ACTORS_PROJECT_ID: "example",
            DURABLE_ACTORS_SECRET: "secret",
            DURABLE_ACTORS_CONTROL_PLANE_URL: `http://127.0.0.1:${port}`
        }
    })
}
