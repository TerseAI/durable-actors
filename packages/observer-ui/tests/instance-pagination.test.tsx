import React, { act } from "react"

import assert from "node:assert/strict"
import { afterEach, test } from "node:test"

import type { ActorInventory, ObserverClient } from "../src/client.js"

import "./dom.js"

const { cleanup, fireEvent, render, within } = await import("@testing-library/react")
const { ActorObserver } = await import("../src/index.js")
afterEach(cleanup)

function inventory(count: number): ActorInventory {
    return {
        connectionsComplete: false,
        actors: [
            {
                actorName: "Room",
                live: count,
                dormant: 0,
                unknown: 0,
                instances: Array.from({ length: count }, (_, index) => ({ actorId: `room-${String(index).padStart(3, "0")}`, status: "live", lastActiveAtMs: 1000 - index, connections: [] }))
            }
        ]
    }
}

test("large inventories bound rendered rows and residency lanes while every instance remains reachable", async () => {
    const snapshot = inventory(250)
    const client: ObserverClient = {
        checkConnection: async () => {},
        listActors: async () => snapshot,
        listResidency: async () => ({ periods: snapshot.actors[0]!.instances.map(instance => ({ actorId: instance.actorId, status: "live", fromMs: 1000, toMs: 2000 })), truncated: true })
    }
    const view = render(<ActorObserver client={client} initialActorName="Room" timeRange={{ kind: "absolute", fromMs: 1000, toMs: 2000 }} />)
    const table = await view.findByRole("table", { name: "Room instances" })
    const timeline = await view.findByRole("group", { name: "Instance residency timeline" })
    const seen: string[] = []
    for (const size of [100, 100, 50]) {
        const ids = within(table)
            .getAllByRole("row")
            .slice(1)
            .map(row => within(row).getByRole("button").textContent!)
        assert.equal(ids.length, size)
        assert.deepEqual(
            [...timeline.querySelectorAll(".socket-timeline-label strong")].map(label => label.textContent),
            ids
        )
        seen.push(...ids)
        if (seen.length < 250) fireEvent.click(view.getByRole("button", { name: "Next instances" }))
    }
    assert.deepEqual(
        seen,
        snapshot.actors[0]!.instances.map(instance => instance.actorId)
    )
    assert.ok(view.getByRole("button", { name: "Next instances" }).hasAttribute("disabled"))
    assert.match(view.container.textContent!, /latest 10,000 reports/)
    fireEvent.click(view.getByRole("button", { name: "room-249" }))
    assert.ok(view.getByRole("heading", { level: 1, name: "room-249" }))
    fireEvent.click(view.getByRole("button", { name: "Back to instances" }))
    assert.ok(within(view.getByRole("table", { name: "Room instances" })).getByRole("button", { name: "room-249" }))
    fireEvent.input(view.getByRole("searchbox", { name: "Search instances" }), { target: { value: "room-000" } })
    assert.equal(within(view.getByRole("table", { name: "Room instances" })).getAllByRole("row").length, 2)
    assert.ok(view.getByRole("button", { name: "room-000" }))
})

test("live refresh preserves the page and filters, updates rows, and clamps pages after inventory shrinks", async () => {
    let publish: (value: ActorInventory) => void = () => {}
    const client: ObserverClient = {
        checkConnection: async () => {},
        listActors: async () => inventory(250),
        watchActors: async receive => {
            publish = receive
            receive(inventory(250))
            await new Promise(() => {})
        }
    }
    const view = render(<ActorObserver client={client} initialActorName="Room" />)
    await view.findByRole("table", { name: "Room instances" })
    fireEvent.click(view.getByRole("button", { name: "Next instances" }))
    const updated = inventory(250)
    updated.actors[0]!.instances[100]!.connections = [{ id: "new-socket", metadata: null }]
    await act(async () => publish(updated))
    assert.match(view.getByRole("row", { name: /room-100/ }).textContent!, /room-100Live1/)
    assert.equal(view.queryByRole("button", { name: "room-000" }), null)
    fireEvent.change(view.getByRole("combobox", { name: "Instance state" }), { target: { value: "live" } })
    assert.ok(view.getByRole("button", { name: "room-000" }))
    fireEvent.click(view.getByRole("button", { name: "Next instances" }))
    fireEvent.click(view.getByRole("button", { name: "Next instances" }))
    await act(async () => publish(inventory(101)))
    assert.ok(view.getByRole("button", { name: "room-100" }))
    assert.equal(within(view.getByRole("table", { name: "Room instances" })).getAllByRole("row").length, 2)
    assert.equal((view.getByRole("combobox", { name: "Instance state" }) as HTMLSelectElement).value, "live")
    assert.ok(view.getByRole("button", { name: "Next instances" }).hasAttribute("disabled"))
})
