import React, { act } from "react"

import assert from "node:assert/strict"
import { afterEach, test } from "node:test"

import type { ActorInventory, ObserverClient, ObserverQuery } from "../src/client.js"
import type { SocketSession } from "../src/socket-sessions.js"

import "./dom.js"

const { cleanup, fireEvent, render, waitFor, within } = await import("@testing-library/react")
const { WebSocketObserver } = await import("../src/WebSocketObserver.js")
const { packLanes, timelineTicks } = await import("../src/SocketTimeline.js")
const { rangeLabel } = await import("../src/time-range.js")
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
    fireEvent.click(view.getByRole("button", { name: "Time range: Last hour" }))
    fireEvent.click(view.getByRole("button", { name: "Last 15 minutes" }))
    await waitFor(() => assert.equal(queries.length, 2))
    assert.ok(queries[1]!.params[0]! >= now - 16 * 60_000, "choosing a preset re-queries with the new lower bound")
    assert.match(view.container.textContent!, /3 sessions in the last 15 minutes/u)
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
    const tooltip = view.getByRole("tooltip")
    assert.match(tooltip.textContent!, /^nameGraceroleguestConnectionclosed-c…7890InstanceRoom \/ general/u, "hovering a bar leads with who the connection belongs to, one entry per line")
    assert.equal(within(tooltip).getAllByRole("term").length, 8)
    fireEvent.mouseLeave(timeline)
    assert.equal(view.queryByRole("tooltip"), null)
    fireEvent.change(view.getByRole("combobox", { name: "Filter by status" }), { target: { value: "closed" } })
    assert.equal(within(timeline).getAllByRole("button").length, 1)
    assert.equal(within(table).getAllByRole("row").length, 2)
    fireEvent.change(view.getByRole("combobox", { name: "Filter connections" }), { target: { value: "nothing" } })
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
    assert.equal(view.queryByRole("button", { name: "Time range: Last hour" }), null)
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

test("a connection reported by a host before its connect trace is saved starts where it was first seen, not at the window edge", async () => {
    let publish: (inventory: ActorInventory) => void = () => {}
    const queries: ObserverQuery[] = []
    const client: ObserverClient = {
        listActors: async () => inventory,
        checkConnection: async () => {},
        watchActors: async (onInventory, signal) => {
            publish = onInventory
            onInventory(inventory)
            await new Promise<void>(resolve => signal.addEventListener("abort", () => resolve(), { once: true }))
        },
        query: async query => {
            queries.push(query)
            return { rows: rows.slice(1, 2), truncated: false }
        }
    }
    const view = render(<WebSocketObserver client={client} />)
    await view.findByRole("button", { name: "Inspect connection closed-connection-1234567890" })
    const timeline = view.getByRole("group", { name: "Connection timeline" })
    const preexisting = within(timeline).getByRole("button", { name: /open-con…7890/u })
    assert.ok(preexisting.className.includes("socket-bar-unknown-start"), "connections already open when the page loads have an unknown start")
    const before = queries.length
    await act(async () =>
        publish({
            actors: [
                {
                    ...inventory.actors[0]!,
                    instances: [
                        { ...inventory.actors[0]!.instances[0]!, connections: [...inventory.actors[0]!.instances[0]!.connections, { id: "fresh-connection-1234567890", metadata: { name: "Linus" } }] }
                    ]
                }
            ]
        })
    )
    const fresh = await within(timeline).findByRole("button", { name: /fresh-co…7890/u })
    assert.ok(!fresh.className.includes("socket-bar-unknown-start"), "a connection that appears later starts when it was first seen")
    assert.match(fresh.getAttribute("aria-label")!, /, (\d+ ms|[0-5](\.\d)? s)$/u, "its duration counts from when it appeared, not from the window start")
    assert.match(view.getByRole("row", { name: /fresh-co…7890/u }).textContent!, /≈ \d\d:\d\d:\d\d/u, "the table marks the start as approximate")
    await waitFor(() => assert.ok(queries.length > before, "an inventory change refreshes history immediately"))
})

test("a custom range is picked from the calendar and time fields, validated, and bounds the session query", async context => {
    // Keep the initial one-hour range on the same calendar day, even when CI runs just after midnight.
    context.mock.timers.enable({ apis: ["Date"], now: new Date(2026, 8, 22, 12).getTime() })
    const queries: ObserverQuery[] = []
    const client: ObserverClient = {
        listActors: async () => inventory,
        checkConnection: async () => {},
        query: async query => {
            queries.push(query)
            return { rows, truncated: false }
        }
    }
    const view = render(<WebSocketObserver client={client} />)
    await view.findByRole("button", { name: "Inspect connection closed-connection-1234567890" })
    fireEvent.click(view.getByRole("button", { name: "Time range: Last hour" }))
    const panel = view.getByRole("dialog", { name: "Time range" })
    fireEvent.change(within(panel).getByLabelText("From"), { target: { value: "09:00" } })
    fireEvent.change(within(panel).getByLabelText("To"), { target: { value: "08:00" } })
    fireEvent.click(within(panel).getByRole("button", { name: "Apply range" }))
    assert.ok(within(panel).getByRole("alert"), "a same-day range that ends before it starts is rejected")
    fireEvent.click(within(panel).getByRole("button", { name: /previous month/iu }))
    const today = new Date()
    const start = new Date(today.getFullYear(), today.getMonth() - 1, 5)
    const end = new Date(today.getFullYear(), today.getMonth() - 1, 6)
    fireEvent.click(panel.querySelector(`[data-day="${start.toLocaleDateString()}"]`)!)
    fireEvent.click(panel.querySelector(`[data-day="${end.toLocaleDateString()}"]`)!)
    fireEvent.change(within(panel).getByLabelText("From"), { target: { value: "07:30" } })
    fireEvent.click(within(panel).getByRole("button", { name: "Apply range" }))
    await waitFor(() => assert.equal(queries.length, 2))
    const custom = { kind: "absolute" as const, fromMs: new Date(start.getFullYear(), start.getMonth(), 5, 7, 30).getTime(), toMs: new Date(end.getFullYear(), end.getMonth(), 6, 8, 0).getTime() }
    assert.deepEqual(queries[1]!.params, [custom.fromMs, custom.toMs], "a custom range binds both ends from the calendar and time fields")
    assert.ok(view.getByRole("button", { name: `Time range: ${rangeLabel(custom)}` }))
    assert.match(view.getByRole("group", { name: "Connection timeline" }).textContent!, /08:00/u, "the timeline ends at the range end instead of now")
    assert.match(view.container.textContent!, new RegExp(`sessions between ${rangeLabel(custom).replace(/[.*+?^${}()|[\]\\]/gu, "\\$&")}`, "u"))
})
