import assert from "node:assert/strict"
import { test } from "node:test"
import { setImmediate } from "node:timers/promises"

import { createClient } from "./client.js"
import type { ActorDescriptor } from "./types.js"

const room: ActorDescriptor<{ text: string }, { text: string }, { messages: string[]; title?: string }, "messages"> = {
    actorType: "Room",
    emittable: ["messages"]
}

test("rejects unsupported actor IDs immediately", () => {
    const env = harness()
    for (const id of ["", "with/slash", "a".repeat(129)]) assert.throws(() => env.client.Room.get(id), /actor ID/i)
    assert.equal(env.requests.length, 0)
})

test("hides authorization, sends payloads for host validation, and caches field subscriptions", async () => {
    const env = harness()
    const connection = env.client.Room.get("lobby")
    const states: string[][] = []
    connection.subscribe("messages", value => states.push(value))
    const connecting = connection.connect()
    await setImmediate()
    assert.deepEqual(env.requests, ["/api/socket/Room/lobby"])
    const socket = env.sockets[0]!
    socket.open()
    assert.deepEqual(socket.sent, [{ type: "authorize", key: "key" }])
    socket.receive({ type: "state", state: { messages: ["initial"], title: "Room" }, version: 1 })
    socket.receive({ type: "ready", protocol: 1, connectionId: "connection", expiresInMs: 1000 })
    await connecting
    assert.equal(connection.status, "open")
    assert.deepEqual(states, [["initial"]])
    connection.send({ text: "hello" })
    assert.deepEqual(socket.sent.at(-1), { type: "message", data: { text: "hello" } })
    connection.send({ text: 1 } as never)
    assert.deepEqual(socket.sent.at(-1), { type: "message", data: { text: 1 } })
    socket.receive({ type: "state_update", changes: { messages: ["new"] }, removed: [], version: 2 })
    socket.receive({ type: "state_update", changes: { messages: ["old"] }, removed: [], version: 1 })
    assert.deepEqual(states, [["initial"], ["new"]])
    const later: string[][] = []
    connection.subscribe("messages", value => {
        later.push(value)
        value.push("mutated by consumer")
    })
    assert.deepEqual(connection.state?.messages, ["new"])
    assert.equal(later.length, 1)
    connection.close()
})

test("renews the existing socket and reconnects with fresh authorization after transport loss", async () => {
    const env = harness()
    const connection = env.client.Room.get("lobby")
    const connecting = connection.connect()
    await setImmediate()
    env.ready(env.sockets[0]!)
    await connecting
    await env.advance(800)
    assert.equal(env.sockets.length, 1)
    assert.equal(env.requests.at(-1), "/api/socket/Room/lobby")
    assert.deepEqual(env.sockets[0]!.sent.at(-1), { type: "renew", key: "key" })
    env.sockets[0]!.receive({ type: "renewed", expiresInMs: 1000 })
    env.sockets[0]!.disconnect(1006)
    assert.equal(connection.status, "reconnecting")
    assert.throws(() => connection.send({ text: "offline" }), /not open/i)
    await env.advance(1000)
    assert.equal(env.sockets.length, 2)
    assert.equal(env.requests.at(-1), "/api/socket/Room/lobby")
    env.ready(env.sockets[1]!)
    assert.equal(connection.status, "open")
    assert.equal(
        env.sockets[1]!.sent.some(frame => frame.type === "message"),
        false
    )
    connection.close()
    await env.advance(10000)
    assert.equal(env.sockets.length, 2)
})

test("permission denial stops retries and a close cancels an outstanding grant request", async () => {
    const denied = harness(async () => new Response(null, { status: 403 }))
    const connection = denied.client.Room.get("lobby")
    await assert.rejects(connection.connect(), /403/)
    await denied.advance(10000)
    assert.equal(denied.requests.length, 1)
    assert.equal(connection.status, "error")
    let resolve!: (response: Response) => void
    const pending = harness(
        () =>
            new Promise<Response>(done => {
                resolve = done
            })
    )
    const room = pending.client.Room.get("lobby")
    const connecting = room.connect()
    const rejected = assert.rejects(connecting, /closed/i)
    room.close()
    resolve(Response.json({ websocketUrl: "ws://example.test/v1/socket", key: "key" }))
    await rejected
    await setImmediate()
    assert.equal(pending.sockets.length, 0)
})

test("invalid protocol frames stop the connection", async () => {
    const env = harness()
    const connection = env.client.Room.get("lobby")
    const messages: unknown[] = []
    connection.on("message", message => messages.push(message))
    const connecting = connection.connect()
    await setImmediate()
    env.ready(env.sockets[0]!)
    await connecting
    env.sockets[0]!.receive({ type: "unknown_frame", data: { text: "hello" } })
    assert.equal(connection.status, "error")
    assert.deepEqual(messages, [])
    await env.advance(10000)
    assert.equal(env.sockets.length, 1)
})

test("retries temporary renewal failures on the same socket until authorization expires", async () => {
    let requests = 0
    const env = harness(async () =>
        ++requests === 2
            ? new Response(null, { status: 503 })
            : Response.json({ websocketUrl: "ws://example.test/v1/socket", key: "fresh" })
    )
    const connection = env.client.Room.get("lobby")
    const connecting = connection.connect()
    await setImmediate()
    env.ready(env.sockets[0]!)
    await connecting
    await env.advance(800)
    assert.equal(connection.status, "open")
    connection.send({ text: "still authorized" })
    await env.advance(200)
    assert.throws(() => connection.send({ text: "expired" }), /not open/i)
    assert.equal(env.requests.length, 2, "must not request renewal once authorization has expired")
    assert.equal(connection.status, "reconnecting")
    connection.close()
})

test("explicit close from a transport close listener prevents reconnection", async () => {
    const env = harness()
    const connection = env.client.Room.get("lobby")
    const connecting = connection.connect()
    await setImmediate()
    env.ready(env.sockets[0]!)
    await connecting
    connection.on("close", () => connection.close())
    env.sockets[0]!.disconnect(1006)
    assert.equal(connection.status, "closed")
    await env.advance(10000)
    assert.equal(env.requests.length, 1)
})

function harness(response?: () => Promise<Response>) {
    let now = 0
    let nextTimer = 0
    const timers = new Map<number, { at: number; callback: () => void }>()
    const sockets: FakeSocket[] = []
    const requests: unknown[] = []
    const client = createClient(
        { Room: room },
        {},
        {
            fetch: async (url, init) => {
                assert.equal(init!.body, undefined)
                assert.equal(init!.method, "POST")
                requests.push(String(url))
                return response
                    ? response()
                    : Response.json({ websocketUrl: "ws://example.test/v1/socket", key: "key" })
            },
            connectWebSocket: () => {
                const socket = new FakeSocket()
                sockets.push(socket)
                return socket
            },
            now: () => now,
            random: () => 0,
            schedule: (callback, ms) => {
                const id = ++nextTimer
                timers.set(id, { at: now + ms, callback })
                return id
            },
            cancel: id => {
                timers.delete(id as number)
            }
        }
    )
    return {
        client,
        sockets,
        requests,
        ready(socket: FakeSocket) {
            socket.open()
            socket.receive({ type: "state", state: { messages: [] }, version: 1 })
            socket.receive({ type: "ready", protocol: 1, connectionId: "connection", expiresInMs: 1000 })
        },
        async advance(ms: number) {
            now += ms
            const due = [...timers].filter(([, timer]) => timer.at <= now)
            for (const [id, timer] of due) {
                timers.delete(id)
                timer.callback()
            }
            await setImmediate()
        }
    }
}

class FakeSocket extends EventTarget {
    readyState = 0
    readonly sent: any[] = []
    send(data: string) {
        assert.equal(this.readyState, 1)
        this.sent.push(JSON.parse(data))
    }
    open() {
        this.readyState = 1
        this.dispatchEvent(new Event("open"))
    }
    receive(data: unknown) {
        this.dispatchEvent(new MessageEvent("message", { data: JSON.stringify(data) }))
    }
    close(code = 1000, reason = "") {
        this.disconnect(code, reason)
    }
    disconnect(code: number, reason = "") {
        this.readyState = 3
        this.dispatchEvent(Object.assign(new Event("close"), { code, reason, wasClean: code === 1000 }))
    }
}
