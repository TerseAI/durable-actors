import assert from "node:assert/strict"
import { on } from "node:events"
import { mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { test } from "node:test"
import { fileURLToPath } from "node:url"
import WebSocket from "ws"

import { createActorTransport } from "../../dist/backend.js"
import { startLocalActors } from "../../dist/localRuntime.js"
import { SocketProxy } from "../../dist/proxy.js"

test("committed receipts survive restart and duplicate events do not repeat writes", { timeout: 120_000 }, async t => {
    const root = await mkdtemp(path.join(os.tmpdir(), "commit-delivery-"))
    const sdk = fileURLToPath(new URL("../..", import.meta.url))
    const sockets = new Set()
    let runtime
    t.after(async () => {
        for (const socket of sockets) socket.terminate()
        await runtime?.stop()
        await rm(root, { recursive: true, force: true })
    })
    await mkdir(path.join(root, "node_modules"))
    await symlink(sdk, path.join(root, "node_modules/durable-actors"))
    await writeFile(path.join(root, "package.json"), JSON.stringify({ type: "module" }))
    await writeFile(
        path.join(root, "tsconfig.json"),
        JSON.stringify({
            compilerOptions: {
                target: "ES2022",
                module: "NodeNext",
                strict: true,
                skipLibCheck: true,
                typeRoots: [path.join(sdk, "node_modules/@types")]
            }
        })
    )
    await writeFile(
        path.join(root, "actors.ts"),
        `import { Actor, Persisted, type ActorSocket } from "durable-actors"
export class Receipts extends Actor {
    @Persisted private records: Record<string, number> = {}
    @Persisted private pending: Record<string, number> = {}
    async record(id: string, fail = false): Promise<number> {
        if (this.records[id] === undefined) this.records[id] = Object.keys(this.records).length + 1
        const value = this.records[id]!
        this.pending[id] = value
        this.broadcastAfterCommit({ type: "receipt", id, value })
        if (fail) throw new Error("rejected")
        return value
    }
    async confirm(id: string): Promise<void> { delete this.pending[id] }
    async read(): Promise<Record<string, number>> { return this.records }
    async crash(): Promise<void> {
        this.records.crash = 999
        this.broadcastAfterCommit({ type: "receipt", id: "crash", value: 999 })
        process.exit(17)
    }
    async onConnect(socket: ActorSocket): Promise<void> {
        for (const [id, value] of Object.entries(this.pending)) socket.sendAfterCommit({ type: "receipt", id, value })
        socket.sendAfterCommit({ type: "ready" })
    }
}`
    )
    const start = () => startLocalActors({ projectId: "receipts", project: root, entrypoint: "actors.ts", quiet: true })
    runtime = await start()
    const invoke = (method, ...args) => {
        const { projectId, controlPlaneUrl, apiKey } = runtime.connection
        return createActorTransport({ projectId, controlPlaneUrl, apiKey }).invoke("Receipts", "one", method, args)
    }
    const connect = async () => {
        const proxy = new SocketProxy({ Receipts: {} }, runtime.connection)
        const grant = await proxy.handle({ actorName: "Receipts", actorId: "one", metadata: null })
        const socket = new WebSocket(grant.websocketUrl)
        sockets.add(socket)
        const messages = on(socket, "message", { signal: t.signal })
        return { socket, next: async () => JSON.parse((await messages.next()).value[0].toString()) }
    }
    const first = await connect()
    assert.deepEqual(await first.next(), { type: "ready" })
    assert.equal(await invoke("record", "first"), 1)
    assert.deepEqual(await first.next(), { type: "receipt", id: "first", value: 1 })
    await assert.rejects(invoke("record", "failed", true), /rejected/)
    assert.equal(await invoke("record", "second"), 2)
    assert.deepEqual(await first.next(), { type: "receipt", id: "second", value: 2 })
    assert.deepEqual(await invoke("read"), { first: 1, second: 2 })
    first.socket.terminate()
    assert.equal(await invoke("record", "offline"), 3)
    await assert.rejects(invoke("crash"))
    // Reconnect uses committed state even after the executor exits before returning a reply.
    assert.deepEqual(await invoke("read"), { first: 1, second: 2, offline: 3 })
    await runtime.stop()
    runtime = await start()
    const replay = await connect()
    const receipts = [await replay.next(), await replay.next(), await replay.next()]
    assert.deepEqual(receipts.map(value => value.id).sort(), ["first", "offline", "second"])
    assert.deepEqual(await replay.next(), { type: "ready" })
    assert.equal(await invoke("record", "offline"), 3)
    assert.deepEqual(await replay.next(), { type: "receipt", id: "offline", value: 3 })
    for (const id of ["first", "second", "offline"]) await invoke("confirm", id)
    replay.socket.terminate()
    const confirmed = await connect()
    assert.deepEqual(await confirmed.next(), { type: "ready" })
})
