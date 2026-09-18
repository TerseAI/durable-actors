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

test("persistence failures warn without hiding live requests", async () => {
    const client = {
        watchRequests: async (receive: (page: RequestTracePage) => void, signal: AbortSignal) => {
            receive({ ...page, persistenceFailed: true })
            await new Promise<void>(resolve => signal.addEventListener("abort", () => resolve(), { once: true }))
        }
    }
    const view = render(<RequestObserver client={client} />)
    assert.ok(await view.findByText("25 ms"))
    assert.match(view.getByRole("alert").textContent!, /could not be saved.*incomplete/u)
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

function sqlRows(records = page.records) {
    return { rows: records.map(record => ({ generation: "one", watermark: 200, pruned: 0, total: 200, sequence: record.sequence, event: JSON.stringify(record) })), truncated: false }
}

test("saved history sends SQL and loads older pages without mixing live rows", async () => {
    const queries: { sql: string; params: unknown[] }[] = []
    const client = {
        watchRequests: async (receive: (page: RequestTracePage) => void, signal: AbortSignal) => {
            receive(page)
            await new Promise<void>(resolve => signal.addEventListener("abort", () => resolve(), { once: true }))
        },
        query: async (query: { sql: string; params: unknown[] }) => {
            queries.push(query)
            if (queries.length === 1) return sqlRows(Array.from({ length: 101 }, (_, i) => ({ ...page.records[0]!, sequence: 200 - i, eventId: `saved-${i}`, operation: `saved call ${i}` })))
            return sqlRows([{ ...page.records[0]!, sequence: 100, eventId: "older", operation: "older call" }])
        }
    }
    const view = render(<RequestObserver client={client} />)
    await view.findByText("post")
    fireEvent.click(view.getByRole("button", { name: "History" }))
    await view.findByText("saved call 0")
    assert.equal(view.queryByText("post"), null)
    fireEvent.click(view.getByRole("button", { name: "Load older" }))
    await view.findByText("older call")
    assert.ok(view.getByText("saved call 0"))
    assert.match(queries[0]!.sql, /FROM request_events/u)
    assert.match(queries[1]!.sql, /sequence <= \?/u)
    assert.deepEqual(queries[1]!.params, [200, 1000, 101])
    fireEvent.click(view.getByRole("button", { name: "Live" }))
    await view.findByText("post")
    assert.equal(view.queryByText("saved call 0"), null)
})

test("HTTP query sends SQL and parameters in JSON without exposing credentials", async () => {
    const query = { sql: "SELECT COUNT(*) AS count FROM request_events WHERE actor_id = ?", params: ["a/b"] }
    const client = new HttpObserverClient("/api/observe", async (url, options) => {
        assert.equal(url, "/api/observe/query")
        assert.equal(options?.method, "POST")
        assert.deepEqual(JSON.parse(String(options?.body)), query)
        assert.equal(new Headers(options?.headers).get("authorization"), null)
        return Response.json({ rows: [{ count: 2 }], truncated: false })
    })
    assert.deepEqual(await client.query(query), { rows: [{ count: 2 }], truncated: false })
})

test("live reconnect resumes after the last received cursor and preserves distinct events", async () => {
    const cursors: (string | undefined)[] = []
    const client = {
        watchRequests: async (receive: (page: RequestTracePage) => void, signal: AbortSignal, after?: string) => {
            cursors.push(after)
            receive({ ...page, resumeCursor: "saved-cursor", records: [{ ...page.records[0]!, eventId: "one" }] })
            if (cursors.length === 1) throw new Error("disconnected")
            receive({ ...page, cursor: 9, evicted: 8, records: [{ ...page.records[0]!, eventId: "two", sequence: 9, operation: "reconnected call" }] })
            await new Promise<void>(resolve => signal.addEventListener("abort", () => resolve(), { once: true }))
        }
    }
    const view = render(<RequestObserver client={client} />)
    await view.findByText("reconnected call", {}, { timeout: 3000 })
    assert.deepEqual(cursors, [undefined, "saved-cursor"])
    assert.ok(view.getByText("post"))
})

test("changing history filters cancels the old query and ignores a late response", async () => {
    Object.assign(globalThis, { FormData: dom.window.FormData })
    let resolveFirst!: (page: ReturnType<typeof sqlRows>) => void
    let firstSignal: AbortSignal | undefined
    const queries: unknown[] = []
    const client = {
        query: async (query: unknown, signal?: AbortSignal) => {
            queries.push(query)
            if (queries.length === 1) {
                firstSignal = signal
                return new Promise<ReturnType<typeof sqlRows>>(resolve => {
                    resolveFirst = resolve
                })
            }
            return sqlRows([{ ...page.records[0]!, operation: "filtered call" }])
        }
    }
    const view = render(<RequestObserver client={client} />)
    fireEvent.click(view.getByRole("button", { name: "History" }))
    // A new query can be submitted from the form while a request is outstanding.
    const actor = view.getByLabelText("Actor ID") as HTMLInputElement
    actor.value = "lobby"
    fireEvent.submit(actor.closest("form")!)
    await view.findByText("filtered call")
    assert.equal(firstSignal?.aborted, true)
    assert.deepEqual((queries[1] as { params: string[] }).params, ["lobby"])
    await act(async () => resolveFirst(sqlRows()))
    assert.equal(view.queryByText("post"), null)
})
