import type { ActorInventory, RequestTrace } from "./client.js"

export function requestSummary(records: RequestTrace[]) {
    const attempts = records.filter(record => record.outcome !== "rerouted")
    return {
        count: records.length,
        success: attempts.length ? (100 * attempts.filter(record => record.outcome === "completed").length) / attempts.length : null,
        p95: percentile(attempts.map(record => record.durationMs))
    }
}

export function tracesInWindow(records: RequestTrace[], minutes: number, now: number) {
    return records.filter(record => record.startedAtMs >= now - minutes * 60_000 && record.startedAtMs <= now)
}

export function inventorySummary(inventory: ActorInventory) {
    return inventory.actors.reduce(
        (total, actor) => ({
            live: total.live + actor.live,
            dormant: total.dormant + actor.dormant,
            unknown: total.unknown + actor.unknown,
            connections: total.connections + actor.instances.reduce((sum, instance) => sum + instance.connections.length, 0)
        }),
        { live: 0, dormant: 0, unknown: 0, connections: 0 }
    )
}

export function queueP95(records: RequestTrace[]) {
    return percentile(records.filter(record => record.outcome !== "rerouted" && record.queueWaitMs !== null).map(record => record.queueWaitMs!))
}

function percentile(values: number[]) {
    if (!values.length) return null
    return values.sort((a, b) => a - b)[Math.ceil(values.length * 0.95) - 1]!
}
