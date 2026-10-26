import React, { act } from "react"

import { cleanup, fireEvent, render, waitFor } from "@testing-library/react"
import assert from "node:assert/strict"
import { afterEach, test } from "node:test"

import { HttpObserverClient } from "../src/client.js"
import { type ActorLog, type LogPage, type LogQuery, displayedLogLimit, isLogPage, mergeLogs } from "../src/log-data.js"

import "./dom.js"

const { LogObserver } = await import("../src/LogObserver.js")
afterEach(cleanup)

const record: ActorLog = {
    eventId: "one",
    timestampMs: Date.now(),
    stream: "stdout",
    level: "INFO",
    message: "Started background work",
    fields: { items: 42, nested: { ready: true } },
    projectId: "default",
    actorName: "Counter",
    actorId: "one",
    hostId: "host-one",
    sessionId: "session-one"
}

function page(records: ActorLog[] = [record], extra: Partial<LogPage> = {}): LogPage {
    return { records, nextCursor: null, dropped: 0, persistenceFailed: false, ...extra }
}

function untilAborted(signal: AbortSignal) {
    return new Promise<void>(resolve => signal.addEventListener("abort", () => resolve(), { once: true }))
}

test("live logs appear while history is pending and merge with late history without duplicates", async () => {
    let complete!: (page: LogPage) => void
    let publish!: (page: LogPage) => void
    const client = {
        listLogs: async () =>
            new Promise<LogPage>(resolve => {
                complete = resolve
            }),
        watchLogs: async (receive: typeof publish, signal: AbortSignal) => {
            publish = receive
            return untilAborted(signal)
        }
    }
    const view = render(<LogObserver client={client} />)
    await act(async () => publish(page()))
    assert.ok(view.getByText("Started background work"))
    assert.match(view.container.querySelector("time")!.textContent!, /\d{2}:\d{2}:\d{2}/u)
    assert.ok(view.getByText("Loading log history…"))
    const details = view.getByText("Started background work").closest("details")!
    details.open = true
    fireEvent(details, new Event("toggle"))
    assert.match(details.textContent!, /Background \/ no request/u)
    assert.match(view.getByLabelText("Structured log fields").textContent!, /"items": 42/u)
    await act(async () => complete(page([record, { ...record, eventId: "old", message: "Older log", timestampMs: record.timestampMs - 10 }])))
    assert.equal(view.getAllByText("Started background work").length, 1)
    assert.ok(view.getByText("Older log"))
    await act(async () => publish(page()))
    assert.equal(view.getByRole("table").querySelectorAll("tbody tr").length, 2)
})

test("history failures and dropped records never hide incoming logs", async () => {
    let publish!: (page: LogPage) => void
    const view = render(
        <LogObserver
            client={{
                listLogs: async () => {
                    throw new Error("offline")
                },
                watchLogs: async (receive, signal) => {
                    publish = receive
                    return untilAborted(signal)
                }
            }}
        />
    )
    await view.findByText(/Log history unavailable/u)
    await act(async () => publish(page(undefined, { dropped: 3, persistenceFailed: true })))
    assert.ok(view.getByText("Started background work"))
    assert.ok(view.getByText(/3 logs were not delivered/u))
    assert.ok(view.getByText(/Some logs could not be saved/u))
})

test("live recovery clears an earlier history persistence warning", async () => {
    let publish!: (page: LogPage) => void
    const view = render(
        <LogObserver
            client={{
                listLogs: async () => page([], { persistenceFailed: true }),
                watchLogs: async (receive, signal) => {
                    publish = receive
                    return untilAborted(signal)
                }
            }}
        />
    )
    await view.findByText(/Some logs could not be saved/u)
    await act(async () => publish(page()))
    assert.equal(view.queryByText(/Some logs could not be saved/u), null)
    assert.ok(view.getByText("Started background work"))
})

test("log filters cancel old history and stream without admitting stale responses", async () => {
    let complete!: (page: LogPage) => void
    const requests: { query: LogQuery; signal?: AbortSignal }[] = []
    const streams: { query?: LogQuery; signal: AbortSignal; publish: (page: LogPage) => void }[] = []
    const client = {
        listLogs: async (query: LogQuery, signal?: AbortSignal) => {
            requests.push({ query, signal })
            return requests.length === 1
                ? new Promise<LogPage>(resolve => {
                      complete = resolve
                  })
                : page([])
        },
        watchLogs: async (publish: (page: LogPage) => void, signal: AbortSignal, query?: LogQuery) => {
            streams.push({ publish, signal, query })
            return untilAborted(signal)
        }
    }
    const view = render(<LogObserver client={client} />)
    for (const [label, value] of [
        ["Actor class", "Counter"],
        ["Actor ID", "one"],
        ["Request ID", "request-1"],
        ["Search logs", "hello"]
    ]) {
        fireEvent.change(view.getByLabelText(label!), { target: { value } })
    }
    fireEvent.change(view.getByLabelText("Level"), { target: { value: "WARN" } })
    fireEvent.click(view.getByRole("button", { name: "Search" }))
    await waitFor(() => assert.equal(requests.length, 2))
    assert.equal(requests[0]!.signal?.aborted, true)
    assert.equal(streams[0]!.signal.aborted, true)
    assert.deepEqual(streams[1]!.query, { actorName: "Counter", actorId: "one", requestId: "request-1", search: "hello", level: "WARN", fromMs: requests[1]!.query.fromMs })
    await act(async () => {
        complete(page())
        streams[0]!.publish(page())
    })
    assert.equal(view.queryByText("Started background work"), null)
    await act(async () => streams[1]!.publish(page([{ ...record, requestId: "request-1", message: "hello" }])))
    assert.ok(view.getByText("hello"))
})

test("reconnection backfills history and keeps the already received rows", async () => {
    let connections = 0
    let loads = 0
    const view = render(
        <LogObserver
            client={{
                listLogs: async () => {
                    loads++
                    return page(loads > 1 ? [{ ...record, eventId: "missed", message: "Missed during disconnect" }] : [])
                },
                watchLogs: async (publish, signal) => {
                    publish(page())
                    if (++connections === 1) throw new Error("lost connection")
                    return untilAborted(signal)
                }
            }}
        />
    )
    await view.findByText("Missed during disconnect", {}, { timeout: 3_000 })
    assert.ok(view.getByText("Started background work"))
    assert.equal(loads, 2)
})

test("log buffers retain only the newest distinct events", () => {
    const logs = Array.from({ length: displayedLogLimit + 20 }, (_, i) => ({ ...record, eventId: `event-${i}`, timestampMs: i }))
    const merged = mergeLogs(logs.slice(0, 100), logs)
    assert.equal(merged.length, displayedLogLimit)
    assert.equal(merged[0]!.timestampMs, displayedLogLimit + 19)
    assert.equal(merged.at(-1)!.timestampMs, 20)
})

test("log client encodes history and stream filters and validates structured records", async () => {
    const controller = new AbortController()
    const requests: string[] = []
    const client = new HttpObserverClient("/api/observe", async (url, options) => {
        requests.push(String(url))
        assert.equal(new Headers(options?.headers).get("authorization"), null)
        return String(url).includes("/events") ? new Response(`event: logs\ndata: ${JSON.stringify(page())}\n\n`, { headers: { "content-type": "text/event-stream" } }) : Response.json(page())
    })
    const query = { actorId: "a/b", search: "hello world" }
    assert.deepEqual(await client.listLogs({ ...query, cursor: "next+page" }), page())
    await client.watchLogs(
        received => {
            assert.deepEqual(received, page())
            controller.abort()
        },
        controller.signal,
        query
    )
    assert.deepEqual(requests, ["/api/observe/logs?actorId=a%2Fb&search=hello+world&cursor=next%2Bpage", "/api/observe/logs/events?actorId=a%2Fb&search=hello+world"])
    assert.equal(isLogPage(page([{ ...record, fields: [] as unknown as Record<string, unknown> }])), false)
    assert.equal(isLogPage(page([{ ...record, timestampMs: Number.NaN }])), false)
    assert.equal(isLogPage(page()), true)
})
