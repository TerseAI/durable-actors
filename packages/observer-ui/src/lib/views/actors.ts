import type { ActorInstance, ActorInventory, ActorType } from "../../types/Actors.js"
import type { ObserveEvent } from "../../types/Events.js"

// A gateway this long without showing it is alive has stopped; its connections are lost.
export const gatewayStaleAfterMs = 30_000

type Rows = Map<string, ActorType>

export function applyActorEvents(inventory: ActorInventory, events: ObserveEvent[]): ActorInventory {
    const rows: Rows = new Map(inventory.actors.map(row => [row.actorName, { ...row, instances: [...row.instances] }]))
    const gateways = { ...inventory.gateways }
    for (const event of events) applyActorEvent(rows, gateways, event)
    return { actors: [...rows.values()].sort((a, b) => a.actorName.localeCompare(b.actorName)), gateways }
}

/** The inventory as of `nowMs`: silent instances turn unknown and silent gateways' connections drop. */
export function currentInventory({ actors, gateways }: ActorInventory, nowMs: number): ActorInventory {
    const alive = (gatewayId: string) => {
        const seen = gateways?.[gatewayId]
        return seen === undefined || seen + gatewayStaleAfterMs >= nowMs
    }
    return {
        gateways,
        actors: actors.map(row =>
            counted(
                row,
                row.instances.map(instance => currentInstance(instance, alive, nowMs)).filter(instance => instance.reportedAtMs !== null || instance.connections.length > 0)
            )
        )
    }
}

function applyActorEvent(rows: Rows, gateways: Record<string, number>, event: ObserveEvent) {
    const seen = (gatewayId: string, atMs: number) => (gateways[gatewayId] = Math.max(gateways[gatewayId] ?? 0, atMs))
    switch (event.type) {
        case "instance": {
            const { actorName, actorId, ...report } = event.item
            const index = instanceIndex(rows, actorName, actorId)
            const instance = rows.get(actorName)!.instances[index]!
            if ((instance.reportedAtMs ?? 0) > report.reportedAtMs) return
            const { status, reportedAtMs, staleAtMs, lastActiveAtMs, waiting } = report
            rows.get(actorName)!.instances[index] = { ...instance, status, reportedAtMs, staleAtMs, lastActiveAtMs, waiting }
            return
        }
        case "connectionOpened": {
            const { actorName, actorId, connectionId, gatewayId, metadata, openedAtMs } = event.item
            const index = instanceIndex(rows, actorName, actorId)
            const instance = rows.get(actorName)!.instances[index]!
            if (!instance.connections.some(connection => connection.id === connectionId)) {
                rows.get(actorName)!.instances[index] = { ...instance, connections: [...instance.connections, { id: connectionId, gatewayId, metadata }] }
            }
            seen(gatewayId, openedAtMs)
            return
        }
        case "connectionClosed": {
            const { actorName, actorId, connectionId, gatewayId, closedAtMs } = event.item
            const row = rows.get(actorName)
            const index = row?.instances.findIndex(instance => instance.actorId === actorId) ?? -1
            if (row && index >= 0) row.instances[index] = { ...row.instances[index]!, connections: row.instances[index]!.connections.filter(connection => connection.id !== connectionId) }
            seen(gatewayId, closedAtMs)
            return
        }
        case "gatewayAlive":
            seen(event.item.gatewayId, event.item.atMs)
            return
        case "deployment": {
            const deployed = new Map(event.item.actors.map(actor => [actor.actorName, actor.methods]))
            for (const [actorName, row] of rows) rows.set(actorName, { ...row, methods: deployed.get(actorName) })
            for (const [actorName, methods] of deployed) if (!rows.has(actorName)) rows.set(actorName, { ...emptyRow(actorName), methods })
        }
    }
}

// An instance first seen through a connection is held by its gateway while the actor sleeps.
function instanceIndex(rows: Rows, actorName: string, actorId: string): number {
    if (!rows.has(actorName)) rows.set(actorName, emptyRow(actorName))
    const row = rows.get(actorName)!
    const index = row.instances.findIndex(instance => instance.actorId === actorId)
    if (index >= 0) return index
    row.instances.push({ actorId, status: "dormant", lastActiveAtMs: null, reportedAtMs: null, staleAtMs: null, connections: [], waiting: null })
    return row.instances.length - 1
}

function currentInstance(instance: ActorInstance, alive: (gatewayId: string) => boolean, nowMs: number): ActorInstance {
    const stale = instance.staleAtMs != null && nowMs > instance.staleAtMs
    return {
        ...instance,
        status: stale ? "unknown" : instance.status,
        waiting: stale ? null : instance.waiting,
        connections: instance.connections.filter(connection => alive(connection.gatewayId))
    }
}

function counted(row: ActorType, instances: ActorInstance[]): ActorType {
    const count = (status: ActorInstance["status"]) => instances.filter(instance => instance.status === status).length
    return { ...row, instances, live: count("live"), dormant: count("dormant"), unknown: count("unknown") }
}

function emptyRow(actorName: string): ActorType {
    return { actorName, live: 0, dormant: 0, unknown: 0, instances: [] }
}
