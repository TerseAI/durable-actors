import React from "react"

import { cleanup, fireEvent, render, waitFor, within } from "@testing-library/react"
import { JSDOM } from "jsdom"
import assert from "node:assert/strict"
import { afterEach, test } from "node:test"

import type { ActorInventory, ObserverClient, ObserverQuery } from "../src/client.js"
import type { SocketSession } from "../src/socket-sessions.js"

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
const { WebSocketObserver } = await import("../src/WebSocketObserver.js")
const { packLanes, timelineTicks } = await import("../src/SocketTimeline.js")
afterEach(cleanup)

const now = Date.now()
const inventory: ActorInventory = {
    actors: [
        { actorName: "Room", live: 1, dormant: 0, unknown: 0, instances: [{ actorId: "general", status: "live", connections: [{ id: "open-connection-1234567890", metadata: { name: "Ada" } }] }] }
    ]
}
const rows = [
    {
        connection_id: "open-connection-1234567890",
        actor_name: "Room",
        actor_id: "general",
        host_id: "host-1",
        opened_at_ms: now - 90_000,
        closed_at_ms: null,
        last_seen_ms: now - 60_000,
        messages: 4,
        failures: 0
    },
    {
        connection_id: "closed-connection-1234567890",
        actor_name: "Room",
        actor_id: "general",
        host_id: "host-1",
        opened_at_ms: now - 600_000,
        closed_at_ms: now - 540_000,
        last_seen_ms: now - 540_000,
        messages: 2,
        failures: 1,
        connect_event: JSON.stringify({ operation: "onConnect", metadata: { name: "Grace", role: "guest" } })
    },
    {
        connection_id: "lost-connection-1234567890",
        actor_name: "Room",
        actor_id: "random",
        host_id: "host-2",
        opened_at_ms: now - 300_000,
        closed_at_ms: null,
        last_seen_ms: now - 290_000,
        messages: 1,
        failures: 0
    }
]

test("the WebSocket page pairs saved sessions with live inventory, filters them, and inspects a connection", async () => {
    const queries: ObserverQuery[] = []
    const client: ObserverClient = {
        listActors: async () => inventory,
        checkConnection: async () => {},
        query: async query => {
            queries.push(query)
            return { rows, truncated: false }
        }
    }
    const view = render(<WebSocketObserver client={client} onSelectActor={() => {}} />)
    await view.findByRole("button", { name: "Inspect connection closed-connection-1234567890" })
    assert.equal(view.getByLabelText("Open WebSocket connections").textContent, "1")
    assert.equal(view.getByLabelText("WebSocket sessions").textContent, "3")
    assert.equal(view.getByLabelText("Median session duration").textContent, "1m 0s")
    assert.equal(view.getByLabelText("WebSocket messages").textContent, "7")
    assert.match(queries[0]!.sql, /request_events/u)
    assert.equal(queries[0]!.params.length, 1, "the default window bounds the query")
    const table = view.getByRole("region", { name: "WebSocket connections" })
    const statuses = within(table)
        .getAllByRole("row")
        .slice(1)
        .map(row => row.querySelector(".socket-status")!.textContent)
    assert.deepEqual(statuses, ["Open", "Lost", "Closed"], "open connections lead, then most recent first")
    assert.match(within(table).getByRole("row", { name: /lost-con…7890/u }).textContent!, /≥ 10 s/u)
    assert.match(within(table).getByRole("row", { name: /open-con…7890/u }).textContent!, /"name":"Ada"/u)
    assert.match(within(table).getByRole("row", { name: /closed-c…7890/u }).textContent!, /"name":"Grace"/u, "closed connections keep the metadata saved with their connect event")
    const timeline = view.getByRole("group", { name: "Connection timeline" })
    assert.equal(within(timeline).getAllByRole("button").length, 3)
    assert.ok(within(timeline).getByRole("button", { name: /^Lost connection lost-con…7890 on Room random, ≥ 10 s$/u }))
    const closedBar = within(timeline).getByRole("button", { name: "Closed connection closed-c…7890 (name: Grace · role: guest) on Room general, 1m 0s" })
    fireEvent.mouseEnter(closedBar)
    assert.match(view.getByRole("tooltip").textContent!, /closed-c…7890name: Grace · role: guestRoom \/ general/u, "hovering a bar shows who the connection belongs to")
    fireEvent.mouseLeave(timeline)
    assert.equal(view.queryByRole("tooltip"), null)
    fireEvent.change(view.getByRole("combobox", { name: "Filter by status" }), { target: { value: "closed" } })
    assert.equal(within(timeline).getAllByRole("button").length, 1)
    assert.equal(within(table).getAllByRole("row").length, 2)
    fireEvent.input(view.getByRole("textbox", { name: "Filter connections" }), { target: { value: "nothing" } })
    assert.ok(view.getAllByText("No matching connections").length >= 1)
    fireEvent.click(view.getByRole("button", { name: "Clear filter" }))
    fireEvent.click(view.getByRole("button", { name: "Inspect connection closed-connection-1234567890" }))
    const dialog = await view.findByRole("dialog", { name: "Connection details" })
    assert.match(dialog.textContent!, /closed-connection-1234567890/u)
    assert.match(dialog.textContent!, /Failures1/u)
    assert.match(dialog.textContent!, /host-1/u)
    assert.match(dialog.textContent!, /Duration1m 0s/u)
    assert.match(dialog.textContent!, /"name": "Grace"/u)
    assert.ok(within(dialog).getByRole("button", { name: "Open Room" }))
})

test("without SQL history the page still lists open connections from inventory and without inventory unfinished sessions are lost", async () => {
    const client: ObserverClient = { listActors: async () => inventory, checkConnection: async () => {} }
    const view = render(<WebSocketObserver client={client} />)
    await view.findByRole("button", { name: "Inspect connection open-connection-1234567890" })
    assert.equal(view.queryByRole("group", { name: "Connection timeline" }), null)
    assert.equal(view.queryByRole("combobox", { name: "Time window" }), null)
    assert.equal(view.getByLabelText("WebSocket sessions").textContent, "—")
    assert.match(view.getByRole("row", { name: /open-con…7890/u }).textContent!, /Open/u)
    view.unmount()
    const offline: ObserverClient = {
        listActors: async () => {
            throw new Error("offline")
        },
        checkConnection: async () => {},
        query: async () => ({ rows: rows.slice(0, 1), truncated: false })
    }
    const fallback = render(<WebSocketObserver client={offline} />)
    await waitFor(() => assert.ok(fallback.getAllByRole("alert").length >= 1))
    await fallback.findByRole("button", { name: "Inspect connection open-connection-1234567890" })
    assert.match(fallback.getByRole("row", { name: /open-con…7890/u }).textContent!, /Lost/u)
    assert.equal(fallback.getByLabelText("Open WebSocket connections").textContent, "0")
})

test("timeline lanes pack overlapping sessions per instance and ticks land on round steps", () => {
    const session = (
        connectionId: string,
        actorId: string,
        openedAtMs: number,
        closedAtMs: number | null,
        status: SocketSession["status"] = closedAtMs === null ? "open" : "closed"
    ): SocketSession => ({
        connectionId,
        actorName: "Room",
        actorId,
        hostId: null,
        openedAtMs,
        closedAtMs,
        lastSeenMs: closedAtMs ?? openedAtMs,
        messages: 0,
        failures: 0,
        status
    })
    const lanes = packLanes([session("a", "general", 0, 5_000), session("b", "general", 2_000, 8_000), session("c", "general", 6_000, null), session("d", "random", 1_000, 2_000)], 0, 10_000)
    assert.deepEqual(
        lanes.map(lane => [lane.actorId, lane.rows.map(row => row.map(item => item.session.connectionId))]),
        [
            ["general", [["a", "c"], ["b"]]],
            ["random", [["d"]]]
        ]
    )
    assert.deepEqual(timelineTicks(1_000, 61_000), [10_000, 20_000, 30_000, 40_000, 50_000])
    assert.deepEqual(timelineTicks(0, 3_600_000).length, 5)
})
