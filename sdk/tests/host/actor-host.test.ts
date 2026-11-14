import assert from "node:assert/strict"
import { once } from "node:events"
import { mkdtemp, rm, writeFile } from "node:fs/promises"
import type { Socket } from "node:net"
import { createServer } from "node:net"
import { createInterface } from "node:readline"
import { before, test } from "node:test"
import type { TestContext } from "node:test"
import { fileURLToPath } from "node:url"

import { buildActor } from "../../src/compiler/actor-build.js"
import { ActorSession, parseHostSettings } from "../../src/host/actor-host.js"
import type { DatabaseChannel } from "../../src/host/database-channel.js"
import { HostDatabase } from "../fixtures/host-database.js"
import { databaseChannel } from "../fixtures/session-port.js"

before(
    async () => {
        await actorBundle()
    },
    { timeout: 30_000 }
)

test("runs commands on the host thread and rebuilds the actor after eviction", { timeout: 10_000 }, async t => {
    const database = new HostDatabase()
    const messageIds: (number | undefined)[] = []
    const host = await rustHost(t, databaseChannel(database, messageIds))
    host.send({ type: "command", message_id: 1, command: invoke("increment", [4]) })
    assert.deepEqual(await host.next(), { type: "reply", message_id: 1, reply: { type: "invoked", result: 4 } })
    assert.deepEqual(database.fields(), { count: 4 })
    assert.ok(messageIds.length > 0 && messageIds.every(id => id === 1))

    host.send({ type: "command", message_id: 2, command: invoke("stream", []) })
    for (const data of ["first", "last"])
        assert.deepEqual(await host.next(), {
            type: "socket_effects",
            message_id: 2,
            effects: [
                {
                    type: "broadcast",
                    message: { type: "text", data: JSON.stringify({ delta: data }) },
                    except_connection_ids: [],
                    tags: []
                }
            ]
        })
    assert.deepEqual(await host.next(), { type: "reply", message_id: 2, reply: { type: "invoked", result: 4 } })

    host.send({ type: "command", message_id: 3, command: { type: "evict", graceful: false } })
    assert.deepEqual(await host.next(), { type: "reply", message_id: 3, reply: { type: "evicted" } })
    host.send({ type: "command", message_id: 4, command: invoke("increment", [1]) })
    assert.deepEqual(await host.next(), { type: "reply", message_id: 4, reply: { type: "invoked", result: 5 } })
})

test("a rejected database write fails the method", { timeout: 10_000 }, async t => {
    const database = new HostDatabase()
    const host = await rustHost(t, {
        call(call) {
            if (call.op === "persist_field") throw new Error("durable storage rejected write")
            return database.call(call)
        }
    })
    host.send({ type: "command", message_id: 1, command: invoke("increment", [1]) })
    const { reply } = (await host.next()) as { reply: { code: string; message: string } }
    assert.equal(reply.code, "actor_method_failed")
    assert.match(reply.message, /durable storage rejected write/)
})

test("reports residency once an instance exists and drops it after a stray error", { timeout: 10_000 }, async t => {
    const database = new HostDatabase()
    const host = await rustHost(t, databaseChannel(database), { residency: true })
    assert.deepEqual(await host.next(), { type: "residency", resident: false })
    host.send({ type: "command", message_id: 1, command: invoke("increment", [2]) })
    assert.deepEqual(await host.next(), { type: "residency", resident: true })
    assert.equal(((await host.next()) as { type: string }).type, "reply")
    host.session.fail(new Error("stray timer"))
    assert.deepEqual(await host.next(), { type: "residency", resident: false })
    host.send({ type: "command", message_id: 2, command: invoke("read", []) })
    assert.deepEqual(await host.next(), { type: "residency", resident: true })
    assert.deepEqual(await host.next(), { type: "reply", message_id: 2, reply: { type: "invoked", result: 2 } })
})

test("a stalled actor import times out", { timeout: 5_000 }, async t => {
    const root = await mkdtemp("/tmp/actor-stalled-")
    t.after(() => rm(root, { recursive: true, force: true }))
    await writeFile(`${root}/actors.mjs`, "await new Promise(() => {})")
    const session = new ActorSession(
        parseHostSettings({
            DURABLE_ACTORS_EXECUTOR_SOCKET: `${root}/executor.sock`,
            DURABLE_ACTORS_ENTRYPOINT: `${root}/actors.mjs`,
            DURABLE_ACTORS_HOST_STARTUP_MS: "20"
        }),
        databaseChannel(new HostDatabase())
    )
    await assert.rejects(session.start(), /actor module loading timed out/)
})

test("a missing Rust host fails startup", async () => {
    const session = new ActorSession(
        parseHostSettings({
            DURABLE_ACTORS_EXECUTOR_SOCKET: `/tmp/ta-missing-${process.pid}.sock`,
            DURABLE_ACTORS_ENTRYPOINT: await actorBundle()
        }),
        databaseChannel(new HostDatabase())
    )
    await assert.rejects(session.start(), /could not attach to Rust host/)
})

async function rustHost(t: TestContext, database: DatabaseChannel, options: { residency?: boolean } = {}) {
    const root = await mkdtemp("/tmp/actor-session-")
    const server = createServer()
    const connected = once(server, "connection") as Promise<[Socket]>
    server.listen(`${root}/executor.sock`)
    await once(server, "listening")
    const session = new ActorSession(
        parseHostSettings({
            DURABLE_ACTORS_EXECUTOR_SOCKET: `${root}/executor.sock`,
            DURABLE_ACTORS_ENTRYPOINT: await actorBundle()
        }),
        database
    )
    const startup = session.start()
    const [socket] = await connected
    t.after(async () => {
        socket.destroy()
        await session.waitUntilDisconnected()
        server.close()
        await rm(root, { recursive: true, force: true })
    })
    const lines = createInterface({ input: socket })[Symbol.asyncIterator]()
    const send = (message: unknown) => socket.write(`${JSON.stringify(message)}\n`)
    const next = async (): Promise<unknown> => {
        for (;;) {
            const line = await lines.next()
            assert.equal(line.done, false)
            const message = JSON.parse(line.value) as { type: string }
            if (message.type === "log") continue
            if (options.residency || message.type !== "residency") return message
        }
    }
    assert.deepEqual(await next(), { type: "attach", protocol: 32, actor_names: ["SessionCounter"] })
    send({
        type: "attached",
        protocol: 32,
        actor: { project_id: "default", actor_name: "SessionCounter", actor_id: "one" }
    })
    await startup
    return { session, send, next }
}

function invoke(method: string, args: number[]) {
    return { type: "invoke", request_id: `request-${method}`, method, args }
}

let preparedFixture: Promise<string> | undefined

function actorBundle(): Promise<string> {
    return (preparedFixture ??= (async () => {
        const source = fileURLToPath(new URL("../fixtures/actorSession.ts", import.meta.url))
        const output = fileURLToPath(new URL("../fixtures/actorSession.mjs", import.meta.url))
        await buildActor(source, output, { local: true })
        return output
    })())
}
