import assert from "node:assert/strict"
import { once } from "node:events"
import { mkdtemp, rm, unlink, writeFile } from "node:fs/promises"
import { createServer } from "node:net"
import type { Socket } from "node:net"
import { createInterface } from "node:readline"
import { test } from "node:test"
import { fileURLToPath } from "node:url"

import { ActorSession, parseHostSettings, serializeWithinBytes } from "../../src/host/actor-host.js"
import { ActorWorkerSupervisor } from "../../src/host/worker-supervisor.js"

test("discovers actors only inside the first execution Worker", { timeout: 5_000 }, async () => {
    const root = await mkdtemp("/tmp/actor-discovery-")
    const entrypoint = `${root}/actors.ts`
    const fixture = fileURLToPath(new URL("../fixtures/actorSession.js", import.meta.url))
    await writeFile(`${root}/package.json`, JSON.stringify({ type: "module" }))
    await writeFile(
        entrypoint,
        `import { isMainThread } from "node:worker_threads"; if (isMainThread) throw new Error("customer code loaded in supervisor"); export { SessionCounter } from ${JSON.stringify(fixture)};`
    )
    const server = createServer(socket => {
        const lines = createInterface({ input: socket })
        lines.once("line", line => {
            assert.deepEqual(JSON.parse(line).actor_types, ["SessionCounter"])
            socket.write(`${JSON.stringify({ type: "attached", protocol: 16 })}\n`)
            socket.end()
        })
    })
    server.listen(`${root}/executor.sock`)
    await once(server, "listening")
    const session = new ActorSession(
        parseHostSettings({
            DURABLE_OBJECT_EXECUTOR_SOCKET: `${root}/executor.sock`,
            DURABLE_OBJECT_ENTRYPOINT: entrypoint
        })
    )
    try {
        await session.start()
        await session.waitUntilDisconnected()
    } finally {
        server.close()
        await rm(root, { recursive: true, force: true })
    }
})

test("a stalled actor import times out and closes the Worker", { timeout: 5_000 }, async () => {
    let closed = 0
    const session = new ActorSession(
        parseHostSettings({
            DURABLE_OBJECT_EXECUTOR_SOCKET: `/tmp/ta-unused-${process.pid}.sock`,
            DURABLE_OBJECT_ENTRYPOINT: fileURLToPath(new URL("../fixtures/actorSession.ts", import.meta.url)),
            DURABLE_OBJECT_HOST_STARTUP_MS: "20"
        }),
        () => ({
            ready: () => new Promise(() => {}),
            async handle() {
                return { type: "evicted" }
            },
            close() {
                closed += 1
            }
        })
    )
    await assert.rejects(session.start(), /actor module loading timed out/)
    assert.equal(closed, 1)
})

test("serializes JSON within exact UTF-8 byte limits", () => {
    const values = [
        null,
        true,
        12.5,
        1e30,
        "plain",
        'quote\"slash\\',
        "中",
        "日本語",
        "€",
        "emoji 😀",
        "\ud800",
        ["nested"],
        { 日本語: { value: "中" } }
    ]
    for (const value of values) {
        const bytes = Buffer.byteLength(JSON.stringify(value))
        assert.equal(serializeWithinBytes(value, bytes), JSON.stringify(value))
        assert.equal(serializeWithinBytes(value, bytes - 1), undefined)
    }
    assert.equal(serializeWithinBytes("x".repeat(1024), 100), undefined)
})

test("oversized serialization stops before visiting the rest of a reply", () => {
    let visitedState = false
    const value = {
        result: "x".repeat(1024),
        get state() {
            visitedState = true
            return {}
        }
    }
    assert.equal(serializeWithinBytes(value, 100), undefined)
    assert.equal(visitedState, false)
    const circular = { self: {} }
    circular.self = circular
    assert.equal(serializeWithinBytes(circular, 100), undefined)
})

test("the actor session carries only owned execution commands", async t => {
    const socketPath = `/tmp/ta-session-${process.pid}.sock`
    await removeSocket(socketPath)
    const server = createServer()
    const customerSocketPromise = new Promise<Socket>(resolve => {
        server.once("connection", resolve)
    })
    server.listen(socketPath)
    await once(server, "listening")
    let closed = 0
    const session = new ActorSession(
        parseHostSettings({
            DURABLE_OBJECT_EXECUTOR_SOCKET: socketPath,
            DURABLE_OBJECT_ENTRYPOINT: fileURLToPath(new URL("../fixtures/actorSession.ts", import.meta.url))
        }),
        options => {
            const supervisor = new ActorWorkerSupervisor(options)
            const close = supervisor.close.bind(supervisor)
            t.mock.method(supervisor, "close", () => {
                closed += 1
                close()
            })
            return supervisor
        }
    )
    const startup = session.start()

    try {
        const customerSocket = await Promise.race([customerSocketPromise, startup.then(() => customerSocketPromise)])
        const lines = createInterface({ input: customerSocket, crlfDelay: Infinity })
        const iterator = lines[Symbol.asyncIterator]()

        assert.deepEqual(await readMessage(iterator), {
            type: "attach",
            protocol: 16,
            actor_types: ["SessionCounter"]
        })
        customerSocket.write(`${JSON.stringify({ type: "attached", protocol: 16 })}\n`)
        await startup

        customerSocket.write(
            `${JSON.stringify({
                type: "command",
                message_id: 1,
                command: {
                    type: "invoke",
                    request_id: "request-1",
                    actor: {
                        actor_type: "SessionCounter",
                        actor_id: "counter-1"
                    },
                    method: "increment",
                    args: [4],
                    state: null
                }
            })}\n`
        )
        assert.deepEqual(await readMessage(iterator), {
            type: "reply",
            message_id: 1,
            reply: { type: "invoked", result: 4, state: { count: 4 } }
        })

        customerSocket.write(
            `${JSON.stringify({
                type: "command",
                message_id: 100,
                command: {
                    type: "invoke",
                    request_id: "stream",
                    actor: { actor_type: "SessionCounter", actor_id: "counter-1" },
                    method: "stream",
                    args: [],
                    state: { count: 4 }
                }
            })}\n`
        )
        for (const data of ["first", "last"]) {
            assert.deepEqual(await readMessage(iterator), {
                type: "socket_effects",
                message_id: 100,
                effects: [
                    {
                        type: "broadcast",
                        message: { type: "text", data: JSON.stringify({ delta: data }) },
                        except_connection_ids: [],
                        tags: []
                    }
                ]
            })
            customerSocket.write(`${JSON.stringify({ type: "socket_effects_published", message_id: 100 })}\n`)
        }
        assert.deepEqual(await readMessage(iterator), {
            type: "reply",
            message_id: 100,
            reply: { type: "invoked", result: 4, state: { count: 4 } }
        })

        customerSocket.write(
            `${JSON.stringify({
                type: "command",
                message_id: 2,
                command: {
                    type: "invoke",
                    request_id: "request-2",
                    actor: actorIdentity(),
                    method: "sizedResponse",
                    args: [32 * 1024 * 1024],
                    state: { count: 4 }
                }
            })}\n`
        )
        assert.deepEqual(await readMessage(iterator), {
            type: "reply",
            message_id: 2,
            reply: {
                type: "failed",
                code: "resource_exhausted",
                message: "actor session response exceeds 33554432 bytes"
            }
        })

        customerSocket.write(
            `${JSON.stringify({
                type: "command",
                message_id: 3,
                command: {
                    type: "invoke",
                    request_id: "request-3",
                    actor: actorIdentity(),
                    method: "increment",
                    args: [1],
                    state: { count: 4 }
                }
            })}\n`
        )
        assert.deepEqual(await readMessage(iterator), {
            type: "reply",
            message_id: 3,
            reply: { type: "invoked", result: 5, state: { count: 5 } }
        })

        customerSocket.write(
            `${JSON.stringify({
                type: "command",
                message_id: 4,
                command: {
                    type: "evict",
                    actor: actorIdentity()
                }
            })}\n`
        )
        assert.deepEqual(await readMessage(iterator), {
            type: "reply",
            message_id: 4,
            reply: { type: "evicted" }
        })

        customerSocket.end()
        await session.waitUntilDisconnected()
        assert.equal(closed, 1)
        lines.close()
        server.close()
        await once(server, "close")
    } finally {
        if (server.listening) server.close()
        await removeSocket(socketPath)
    }
})

test("a failed session connection cleans up the speculative Worker", async () => {
    let closed = 0
    const session = new ActorSession(
        parseHostSettings({
            DURABLE_OBJECT_EXECUTOR_SOCKET: `/tmp/ta-missing-${process.pid}.sock`,
            DURABLE_OBJECT_ENTRYPOINT: fileURLToPath(new URL("../fixtures/actorSession.ts", import.meta.url))
        }),
        () => ({
            async ready() {
                return ["SessionCounter"]
            },
            async handle() {
                return { type: "evicted" }
            },
            close() {
                closed += 1
            }
        })
    )
    await assert.rejects(session.start(), /could not attach to Rust host/)
    assert.equal(closed, 1)
})

function actorIdentity(): Record<string, string> {
    return {
        actor_type: "SessionCounter",
        actor_id: "counter-1"
    }
}

async function readMessage(iterator: AsyncIterator<string>): Promise<unknown> {
    const next = await iterator.next()
    assert.equal(next.done, false)
    const message: unknown = JSON.parse(next.value ?? "")
    return message
}

async function removeSocket(socketPath: string): Promise<void> {
    try {
        await unlink(socketPath)
    } catch (error) {
        if (!(error instanceof Error) || !("code" in error) || error.code !== "ENOENT") throw error
    }
}

test("reports resident instances when the Rust host advertises support", { timeout: 5_000 }, async () => {
    const root = await mkdtemp("/tmp/actor-residency-")
    const actor = { actor_type: "SessionCounter", actor_id: "one" }
    let received: unknown
    const server = createServer(socket => {
        const lines = createInterface({ input: socket })
        lines.on("line", line => {
            const message = JSON.parse(line)
            if (message.type === "attach")
                socket.write(`${JSON.stringify({ type: "attached", protocol: 16, supports_residency: true })}\n`)
            else if (message.type === "residency") {
                received = message.actors
                socket.end()
            }
        })
    })
    server.listen(`${root}/executor.sock`)
    await once(server, "listening")
    const session = new ActorSession(
        parseHostSettings({
            DURABLE_OBJECT_EXECUTOR_SOCKET: `${root}/executor.sock`,
            DURABLE_OBJECT_ENTRYPOINT: fileURLToPath(new URL("../fixtures/actorSession.ts", import.meta.url))
        }),
        () => ({
            ready: async () => ["SessionCounter"],
            handle: async () => ({ type: "evicted" }),
            close() {},
            residentActors: () => [actor]
        })
    )
    try {
        await session.start()
        await session.waitUntilDisconnected()
        assert.deepEqual(received, [actor])
    } finally {
        server.close()
        await rm(root, { recursive: true, force: true })
    }
})
