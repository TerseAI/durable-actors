import React, { act } from "react"

import { cleanup, fireEvent, render, waitFor } from "@testing-library/react"
import { JSDOM } from "jsdom"
import assert from "node:assert/strict"
import { afterEach, test } from "node:test"

import { HttpObserverClient } from "../src/client.js"
import type { RequestTracePage } from "../src/client.js"

const dom = new JSDOM("<!doctype html><html><body></body></html>")
Object.assign(globalThis, {
    window: dom.window,
    document: dom.window.document,
    HTMLElement: dom.window.HTMLElement,
    Node: dom.window.Node,
    NodeFilter: dom.window.NodeFilter,
    HTMLInputElement: dom.window.HTMLInputElement,
    MutationObserver: dom.window.MutationObserver,
    CustomEvent: dom.window.CustomEvent,
    getComputedStyle: dom.window.getComputedStyle.bind(dom.window),
    IS_REACT_ACT_ENVIRONMENT: true
})
const { RequestObserver } = await import("../src/RequestObserver.js")
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
            actorName: "Room",
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
    assert.match(queries[0]!.sql, /started_at_ms >= \?/u, "history defaults to the last hour")
    // The bound is floored to the minute, so allow a full extra minute of slack.
    assert.ok(Number(queries[0]!.params[0]) >= Date.now() - 62 * 60_000 && Number(queries[0]!.params[0]) <= Date.now() - 60 * 60_000)
    assert.deepEqual(queries[1]!.params.slice(1), [200, 1000, 101])
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
    assert.deepEqual((queries[1] as { params: string[] }).params.slice(1), ["lobby"])
    await act(async () => resolveFirst(sqlRows()))
    assert.equal(view.queryByText("post"), null)
})

test("instance requests filter both class and ID in live and saved history", async () => {
    const queries: { sql: string; params: unknown[] }[] = []
    const client = {
        watchRequests: async (receive: (value: RequestTracePage) => void, signal: AbortSignal) => {
            receive({
                ...page,
                records: [
                    page.records[0]!,
                    { ...page.records[0]!, sequence: 2, actorName: "Counter", operation: "wrong class" },
                    { ...page.records[0]!, sequence: 3, actorId: "other", operation: "wrong instance" }
                ]
            })
            await new Promise<void>(resolve => signal.addEventListener("abort", () => resolve(), { once: true }))
        },
        query: async (query: { sql: string; params: unknown[] }) => {
            queries.push(query)
            return sqlRows()
        }
    }
    const view = render(<RequestObserver client={client} actor={{ actorName: "Room", actorId: "lobby" }} />)
    await view.findByText("post")
    assert.equal(view.queryByText("wrong class"), null)
    assert.equal(view.queryByText("wrong instance"), null)
    fireEvent.click(view.getByRole("button", { name: "History" }))
    await view.findByRole("table", { name: "Saved requests" })
    assert.match(queries[0]!.sql, /actor_name = \?/u)
    assert.match(queries[0]!.sql, /actor_id = \?/u)
    assert.deepEqual(queries[0]!.params.slice(1), ["Room", "lobby"])
    assert.equal(view.queryByLabelText("Actor ID"), null)
    fireEvent.click(view.getByRole("button", { name: "Last hour" }))
    fireEvent.click(view.getByRole("button", { name: "All retained" }))
    await waitFor(() => assert.equal(queries.length, 2))
    assert.deepEqual(queries[1]!.params, ["Room", "lobby"], "all retained history drops the time bound")
})

test("request inspection keeps table rows intact and opens a separate details sheet", async () => {
    const view = render(
        <RequestObserver
            client={{
                watchRequests: async (receive, signal) => {
                    receive(page)
                    await new Promise<void>(resolve => signal.addEventListener("abort", () => resolve(), { once: true }))
                }
            }}
        />
    )
    await view.findByText("post")
    const table = view.getByRole("table", { name: "Recent requests" })
    fireEvent.click(view.getByRole("button", { name: "Inspect post request" }))
    assert.ok(await view.findByRole("dialog", { name: "Request details" }))
    assert.equal(table.querySelectorAll("tbody tr").length, 1)
    assert.equal(table.textContent!.includes("request-one"), false)
    assert.ok(view.getByText("request-one"))
    fireEvent.click(view.getByRole("button", { name: "Close" }))
    assert.equal(view.queryByRole("dialog"), null)
})
