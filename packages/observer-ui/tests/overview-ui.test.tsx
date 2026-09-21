import React, { act } from "react"

import { cleanup, fireEvent, render, waitFor } from "@testing-library/react"
import { JSDOM } from "jsdom"
import assert from "node:assert/strict"
import { afterEach, test } from "node:test"

import { ConsoleApp } from "../src/ConsoleApp.js"
import { Overview } from "../src/Overview.js"
import type { ActorInventory, ObserverClient, RequestTracePage } from "../src/client.js"
import { requestSummary, tracesInWindow } from "../src/overview-data.js"

const dom = new JSDOM("<!doctype html><html><body></body></html>")
Object.assign(globalThis, { window: dom.window, document: dom.window.document, HTMLElement: dom.window.HTMLElement, IS_REACT_ACT_ENVIRONMENT: true })
afterEach(cleanup)

const inventory: ActorInventory = {
    actors: [
        { actorName: "Room", live: 2, dormant: 1, unknown: 0, instances: [{ actorId: "general", status: "live", connections: [{ id: "socket-a", metadata: null }] }] },
        { actorName: "Counter", live: 0, dormant: 0, unknown: 1, instances: [] }
    ]
}
const page: RequestTracePage = {
    epoch: "first",
    cursor: 2,
    capacity: 500,
    evicted: 0,
    dropped: 0,
    records: [
        {
            sequence: 2,
            requestId: "r2",
            hostId: "h",
            sessionId: "s",
            actorName: "Room",
            actorId: "general",
            kind: "method",
            operation: "post",
            connectionId: null,
            startedAtMs: Date.now(),
            durationMs: 30,
            queueWaitMs: 5,
            outcome: "failed"
        },
        {
            sequence: 1,
            requestId: "r1",
            hostId: "h",
            sessionId: "s",
            actorName: "Room",
            actorId: "general",
            kind: "method",
            operation: "post",
            connectionId: null,
            startedAtMs: Date.now(),
            durationMs: 10,
            queueWaitMs: 0,
            outcome: "completed"
        }
    ]
}

test("overview uses live inventory and retained traces, filters classes, and opens an actor", async () => {
    let receive: (value: RequestTracePage) => void = () => {}
    let requestSignal: AbortSignal | undefined
    let selected = ""
    const client: ObserverClient = {
        listActors: async () => inventory,
        checkConnection: async () => {},
        watchRequests: async (next, signal) => {
            receive = next
            requestSignal = signal
            return new Promise(() => {})
        }
    }
    const view = render(<Overview client={client} onSelectActor={actor => (selected = actor)} />)
    await view.findByRole("button", { name: "Inspect Room" })
    assert.equal(view.getByLabelText("Actor instances").textContent, "4")
    assert.equal(view.getByLabelText("Open WebSocket connections").textContent, "1")
    assert.equal(view.getByLabelText("Retained requests").textContent, "—")
    await act(async () => receive(page))
    assert.equal(view.getByLabelText("Retained requests").textContent, "2")
    assert.match(view.getByRole("button", { name: "Inspect Room" }).closest("tr")!.textContent!, /50%/)
    assert.match(view.container.textContent!, /latest 500/)
    fireEvent.input(view.getByRole("textbox", { name: "Filter actor classes" }), { target: { value: "counter" } })
    assert.ok(!view.queryByRole("button", { name: "Inspect Room" }))
    assert.equal(view.getByLabelText("Actor instances").textContent, "4")
    fireEvent.input(view.getByRole("textbox", { name: "Filter actor classes" }), { target: { value: "room" } })
    fireEvent.click(view.getByRole("button", { name: "Inspect Room" }))
    assert.equal(selected, "Room")
    await act(async () => receive({ ...page, epoch: "restart", records: [], cursor: 0 }))
    assert.equal(view.getByLabelText("Retained requests").textContent, "0")
    view.unmount()
    assert.equal(requestSignal?.aborted, true)
})

test("unavailable sources do not masquerade as zero or successful metrics", async () => {
    const client: ObserverClient = {
        listActors: async () => {
            throw new Error("private")
        },
        checkConnection: async () => {}
    }
    const view = render(<Overview client={client} onSelectActor={() => {}} />)
    await waitFor(() => assert.ok(view.getAllByRole("alert").length >= 1))
    assert.equal(view.getByLabelText("Actor instances").textContent, "—")
    assert.equal(view.getByLabelText("Retained requests").textContent, "—")
    assert.equal(view.getByLabelText("Open WebSocket connections").textContent, "—")
    assert.doesNotMatch(view.container.textContent!, /private|100%/)
})

test("request metrics exclude reroutes and time windows exclude older and future traces", () => {
    const records = page.records.map((record, index) => ({ ...record, startedAtMs: index ? 1000 : 61000 }))
    assert.equal(tracesInWindow(records, 1, 61000).length, 2)
    assert.equal(tracesInWindow(records, 1, 61001).length, 1)
    assert.equal(tracesInWindow(records, 1, 999).length, 0)
    assert.deepEqual(requestSummary([{ ...records[0]!, outcome: "rerouted" }]), { count: 1, success: null, p95: null })
})

test("console navigation opens the selected actor and real WebSocket metadata", async () => {
    const client: ObserverClient = { listActors: async () => inventory, checkConnection: async () => {} }
    const view = render(<ConsoleApp client={client} toggleTheme={() => {}} />)
    const actorLink = await view.findByRole("button", { name: "Inspect Room" })
    actorLink.focus()
    fireEvent.click(actorLink)
    assert.ok(await view.findByRole("heading", { name: "Room instances" }))
    assert.ok(view.getByRole("heading", { name: "Room", level: 1 }))
    assert.equal(view.queryByRole("table", { name: "Actor instance counts" }), null)
    fireEvent.click(view.getByRole("button", { name: "Actors", exact: true }))
    fireEvent.input(await view.findByRole("searchbox", { name: "Search actors" }), { target: { value: "room" } })
    fireEvent.click(view.getByRole("button", { name: "Room", exact: true }))
    assert.ok(view.getByRole("heading", { name: "Room", level: 1 }))
    fireEvent.click(view.getByRole("button", { name: "Actors", exact: true }))
    assert.ok(view.getByRole("table", { name: "Actor instance counts" }))
    assert.equal((view.getByRole("searchbox", { name: "Search actors" }) as HTMLInputElement).value, "room")
    fireEvent.click(view.getByRole("button", { name: "WebSockets", exact: true }))
    assert.ok(await view.findByRole("button", { name: "Inspect connection socket-a" }))
    assert.ok(view.getByText("null"))
    fireEvent.input(view.getByRole("textbox", { name: "Filter connections" }), { target: { value: "missing" } })
    assert.ok(view.getByText("No matching connections"))
    fireEvent.click(view.getByRole("button", { name: "Clear filter" }))
    assert.ok(view.getByRole("button", { name: "Inspect connection socket-a" }))
})
