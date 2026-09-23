import assert from "node:assert/strict"
import { test } from "node:test"

import { HttpObserverClient } from "../src/client.js"

const total = { actorName: "", count: 2, success: 50, p95: 25, queueP95: 10 }
const queue = { actorName: "Room", actorId: "one", admitted: 2, averageMs: 5, maxMs: 10 }
const socket = { connectionId: "socket", actorName: "Room", actorId: "one", hostId: "host", openedAtMs: 10, closedAtMs: 20, lastSeenMs: 20, messages: 1, failures: 0, metadata: { user: "ada" } }

for (const [method, path, body] of [
    ["getMetrics", "metrics", { total, classes: [] }],
    ["listQueueWaits", "queue-waits", [queue]],
    ["listWebSockets", "websockets", [socket]]
] as const) {
    test(`${method} reads its typed endpoint with range filters and cancellation`, async () => {
        const controller = new AbortController()
        const client = new HttpObserverClient("/api/observe", async (url, options) => {
            assert.equal(url, `/api/observe/${path}?fromMs=10&toMs=20`)
            assert.equal(options?.method, "GET")
            assert.equal(options?.signal, controller.signal)
            assert.equal(options?.redirect, "error")
            assert.equal(new Headers(options?.headers).get("authorization"), null)
            return Response.json(body)
        })
        assert.deepEqual(await client[method]({ fromMs: 10, toMs: 20 }, controller.signal), body)
    })
}

test("typed metrics reject invalid responses before displaying data", async () => {
    for (const [method, value] of [
        ["getMetrics", { total: { ...total, success: 101 }, classes: [] }],
        ["listQueueWaits", [{ ...queue, admitted: 0 }]],
        ["listWebSockets", [{ ...socket, messages: -1 }]]
    ] as const) {
        const client = new HttpObserverClient("/api/observe", async () => Response.json(value))
        await assert.rejects(client[method]({}), /Invalid/)
    }
})
