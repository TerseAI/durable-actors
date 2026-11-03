import assert from "node:assert/strict"
import { once } from "node:events"
import { test } from "node:test"
import type { TestContext } from "node:test"
import { WebSocketServer } from "ws"

import { subscribe } from "../../src/browser.js"
import { Subscription } from "../../src/client-runtime/browser.js"

test("subscriptions merge complete isolated states and ignore stale frames and application messages", async t => {
    const states: Record<string, unknown>[] = []
    const { socket } = await setup(t, state => {
        states.push(structuredClone(state))
        if (Array.isArray(state.items)) state.items.push(999)
    })
    socket.message({ type: "state", state: { count: 1, items: [1], label: "present" }, version: 1 })
    socket.message("application message")
    socket.message({ type: "state_update", changes: { count: 2 }, removed: [], version: 3 })
    socket.message({ type: "state_update", changes: { count: 0 }, removed: [], version: 2 })
    socket.message({ type: "state_update", changes: { items: [2] }, removed: ["label"], version: 4 })
    socket.message({ type: "state_update", changes: { label: null }, removed: [], version: 5 })
    assert.deepEqual(states, [
        { count: 1, items: [1], label: "present" },
        { count: 2, items: [1], label: "present" },
        { count: 2, items: [2] },
        { count: 2, items: [2], label: null }
    ])
})

test("reconnect fetches a fresh grant and accepts a new snapshot with a lower version", async t => {
    t.mock.timers.enable({ apis: ["setTimeout"] })
    const states: Record<string, unknown>[] = []
    const { socket, sockets, grants } = await setup(t, state => states.push(state))
    socket.message({ type: "state", state: { count: 99 }, version: 99 })
    socket.dispatchEvent(new Event("error"))
    socket.dispatchEvent(new Event("close"))
    t.mock.timers.tick(1000)
    await flush()
    assert.equal(grants(), 2)
    assert.deepEqual(
        sockets.map(item => item.url),
        ["wss://actor.example/1", "wss://actor.example/2"]
    )
    socket.message({ type: "state", state: { obsolete: true }, version: 100 })
    sockets[1]!.message({ type: "state", state: { count: 1 } })
    sockets[1]!.message({ type: "state_update", changes: { count: 2 }, removed: [], version: 1 })
    assert.deepEqual(states, [{ count: 99 }, { count: 1 }, { count: 2 }])
})

test("grant failures retry with capped backoff and report errors", async t => {
    t.mock.timers.enable({ apis: ["setTimeout"] })
    const errors: unknown[] = []
    let attempts = 0
    const failure = new Error("offline")
    const subscription = new Subscription(
        async () => {
            attempts++
            throw failure
        },
        () => assert.fail("unexpected state"),
        { onError: error => errors.push(error) }
    )
    t.after(() => subscription.close())
    await flush()
    for (const delay of [1000, 2000, 4000, 8000, 16000, 30000, 30000]) {
        const before = attempts
        t.mock.timers.tick(delay - 1)
        await flush()
        assert.equal(attempts, before)
        t.mock.timers.tick(1)
        await flush()
        assert.equal(attempts, before + 1)
    }
    assert.equal(errors.length, attempts)
    assert.ok(errors.every(error => error === failure))
    subscription.close()
    t.mock.timers.tick(30000)
    await flush()
    assert.equal(attempts, 8)
})

test("close during grant fetching prevents a socket from opening", async () => {
    let resolve!: (grant: { websocketUrl: string }) => void
    const pending = new Promise<{ websocketUrl: string }>(done => {
        resolve = done
    })
    const subscription = new Subscription(
        () => pending,
        () => assert.fail("unexpected state"),
        {},
        {
            connectWebSocket: () => assert.fail("closed subscription opened a socket")
        }
    )
    await flush()
    subscription.close()
    subscription.close()
    resolve({ websocketUrl: "wss://actor.example" })
    await flush()
    assert.equal(subscription.closed, true)
})

test("synchronous grant errors are reported after subscribe returns and can stop retries", async t => {
    t.mock.timers.enable({ apis: ["setTimeout"] })
    let attempts = 0
    const subscription = subscribe(
        () => {
            attempts++
            throw new Error("denied")
        },
        () => {},
        { onError: () => subscription.close() }
    )
    await flush()
    assert.equal(subscription.closed, true)
    t.mock.timers.tick(30000)
    await flush()
    assert.equal(attempts, 1)

    const disposed = subscribe(
        () => assert.fail("disposed subscription fetched a grant"),
        () => {}
    )
    disposed.close()
    await flush()
})

test("invalid frames and callback failures stop the subscription", async t => {
    for (const frame of [
        "not JSON",
        { type: "state_update", changes: {}, removed: [], version: 1 },
        { type: "state", state: [], version: 1 },
        { type: "state", state: {}, version: -1 },
        { type: "state", state: {}, version: null },
        { type: "state_update", changes: {}, removed: [], version: "1" },
        { type: "state_update", changes: {}, removed: [1], version: 1 },
        { type: "state", state: { callback: "throw" }, version: 1 }
    ]) {
        const errors: Error[] = []
        const { socket, subscription } = await setup(
            t,
            () => {
                throw new Error("callback failed")
            },
            errors
        )
        if (typeof frame === "string") socket.dispatchEvent(new MessageEvent("message", { data: frame }))
        else socket.message(frame)
        assert.equal(subscription.closed, true)
        assert.equal(socket.closes, 1)
        assert.equal(errors.length, 1)
        assert.equal(subscription.error, errors[0])
    }
})

test("closing from a state or error callback prevents reconnects", async t => {
    t.mock.timers.enable({ apis: ["setTimeout"] })
    const active = await setup(t, () => active.subscription.close())
    active.socket.message({ type: "state", state: {}, version: 1 })
    const failed = await setup(t, () => assert.fail("unexpected state"))
    failed.subscription.close()
    failed.socket.message({ type: "state", state: {}, version: 1 })
    active.socket.dispatchEvent(new Event("close"))
    t.mock.timers.tick(30000)
    await flush()
    assert.equal(active.grants(), 1)
    assert.equal(failed.grants(), 1)

    let attempts = 0
    const rejected = new Subscription(
        async () => {
            attempts++
            throw new Error("access denied")
        },
        () => {},
        { onError: () => rejected.close() }
    )
    await flush()
    t.mock.timers.tick(30000)
    await flush()
    assert.equal(attempts, 1)
    assert.equal(rejected.closed, true)
})

test("public browser API receives state and reconnects over real WebSockets", { timeout: 5000 }, async t => {
    const server = new WebSocketServer({ host: "127.0.0.1", port: 0 })
    t.after(async () => {
        for (const socket of server.clients) socket.terminate()
        await new Promise<void>(resolve => server.close(() => resolve()))
    })
    await once(server, "listening")
    const address = server.address()
    assert.ok(address && typeof address !== "string")
    const urls: string[] = []
    server.on("connection", (socket, request) => {
        urls.push(request.url!)
        socket.send(JSON.stringify({ type: "state", state: { count: urls.length }, version: 0 }))
        if (urls.length === 1) socket.close()
        else socket.send(JSON.stringify({ type: "state_update", changes: { count: 3 }, removed: [], version: 1 }))
    })
    const states: number[] = []
    let calls = 0
    let done!: () => void
    const received = new Promise<void>(resolve => {
        done = resolve
    })
    const subscription = subscribe<{ count: number }>(
        async () => ({ websocketUrl: `ws://127.0.0.1:${address.port}/?grant=${++calls}` }),
        state => {
            states.push(state.count)
            if (state.count === 3) done()
        },
        { onError: () => {} }
    )
    t.after(() => subscription.close())
    await received
    subscription.close()
    assert.deepEqual(states, [1, 2, 3])
    assert.deepEqual(urls, ["/?grant=1", "/?grant=2"])
})

async function setup(t: TestContext, callback: (state: Record<string, unknown>) => void, errors: Error[] = []) {
    const sockets: FakeSocket[] = []
    let calls = 0
    const subscription = new Subscription<Record<string, unknown>>(
        async () => ({ websocketUrl: `wss://actor.example/${++calls}` }),
        callback,
        { onError: error => errors.push(error) },
        {
            connectWebSocket: url => {
                const socket = new FakeSocket(url)
                sockets.push(socket)
                return socket
            }
        }
    )
    t.after(() => subscription.close())
    await flush()
    return { subscription, socket: sockets[0]!, sockets, grants: () => calls }
}

class FakeSocket extends EventTarget {
    closes = 0
    constructor(readonly url: string) {
        super()
    }
    message(value: unknown) {
        this.dispatchEvent(new MessageEvent("message", { data: JSON.stringify(value) }))
    }
    close() {
        this.closes++
    }
}

async function flush() {
    await Promise.resolve()
    await Promise.resolve()
    await Promise.resolve()
}
