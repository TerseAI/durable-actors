import React, { act } from "react"

import { cleanup, fireEvent, render } from "@testing-library/react"
import { JSDOM } from "jsdom"
import assert from "node:assert/strict"
import { afterEach, test } from "node:test"

import { RequestObserver } from "../src/RequestObserver.js"
import { HttpObserverClient } from "../src/client.js"
import type { RequestTracePage } from "../src/client.js"

const dom = new JSDOM("<!doctype html><html><body></body></html>")
Object.assign(globalThis, { window: dom.window, document: dom.window.document, HTMLElement: dom.window.HTMLElement, IS_REACT_ACT_ENVIRONMENT: true })
afterEach(cleanup)
const page: RequestTracePage = {
    epoch: "one",
    cursor: 1,
    capacity: 500,
    evicted: 0,
    dropped: 0,
    records: [
        {
            sequence: 1,
            requestId: "request-one",
            hostId: "host-one",
            sessionId: "session-one",
            actorType: "Room",
            actorId: "lobby",
            kind: "method",
            operation: "post",
            connectionId: null,
            startedAtMs: 1000,
            durationMs: 25,
            queueWaitMs: 10,
            outcome: "completed"
        }
    ]
}

test("request history shows both timings, deduplicates replay, and can pause for inspection", async () => {
    let publish!: (page: RequestTracePage) => void
    const client = {
        watchRequests: async (receive: typeof publish, signal: AbortSignal) => {
            publish = receive
            receive(page)
            await new Promise<void>(resolve => signal.addEventListener("abort", () => resolve(), { once: true }))
        }
    }
    const view = render(<RequestObserver client={client} />)
    assert.ok(await view.findByText("25 ms"))
    assert.ok(view.getByText("10 ms"))
    await act(async () => publish(page))
    assert.equal(view.getAllByRole("button", { name: "Inspect post request" }).length, 1)
    fireEvent.click(view.getByRole("button", { name: "Pause" }))
    const next = {
        ...page,
        cursor: 2,
        dropped: 3,
        records: [{ ...page.records[0]!, sequence: 2, requestId: "request-two", operation: "onMessage", kind: "websocket" as const, outcome: "failed" as const }]
    }
    await act(async () => publish(next))
    assert.equal(view.queryByText("onMessage"), null)
    fireEvent.click(view.getByRole("button", { name: "Resume" }))
    assert.ok(view.getByText("onMessage"))
    assert.match(view.getByRole("alert").textContent!, /3.*not delivered/u)
    fireEvent.click(view.getByRole("button", { name: "Inspect onMessage request" }))
    assert.ok(view.getByText("request-two"))
    await act(async () => publish({ ...page, epoch: "restarted" }))
    assert.equal(view.queryByText("request-two"), null)
    assert.equal(view.getAllByRole("button", { name: "Inspect post request" }).length, 1)
})

test("request streams validate records and use the trace endpoint", async () => {
    const controller = new AbortController()
    const client = new HttpObserverClient("/api/observe", async url => {
        assert.equal(url, "/api/observe/requests/events")
        return new Response(`event: requests\ndata: ${JSON.stringify(page)}\n\n`, { headers: { "content-type": "text/event-stream" } })
    })
    await client.watchRequests(value => {
        assert.deepEqual(value, page)
        controller.abort()
    }, controller.signal)
    const invalid = { ...page, records: [{ ...page.records[0], queueWaitMs: 100 }] }
    const broken = new HttpObserverClient("/api/observe", async () => new Response(`event: requests\ndata: ${JSON.stringify(invalid)}\n\n`, { headers: { "content-type": "text/event-stream" } }))
    await assert.rejects(broken.watchRequests(() => assert.fail("invalid trace accepted"), new AbortController().signal))
})
