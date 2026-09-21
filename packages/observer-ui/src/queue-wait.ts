import type { ObserverQuery, ObserverQueryResult, SqlValue } from "./client.js"
import type { ResolvedRange } from "./time-range.js"

export interface QueueWaitRow {
    actorName: string
    actorId: string
    admitted: number
    averageMs: number
    maxMs: number
}

export interface QueueWaitStats {
    admitted: number
    averageMs: number
    maxMs: number
}

// Queue wait exists only for admitted attempts; reroutes never entered the actor.
export function queueWaitQuery(range: ResolvedRange, actorName?: string): ObserverQuery {
    const clauses = ["queue_wait_ms IS NOT NULL", "outcome <> 'rerouted'"]
    const params: SqlValue[] = []
    if (range.fromMs !== undefined) {
        clauses.push("started_at_ms >= ?")
        params.push(range.fromMs)
    }
    if (range.toMs !== undefined) {
        clauses.push("started_at_ms <= ?")
        params.push(range.toMs)
    }
    if (actorName !== undefined) {
        clauses.push("actor_name = ?")
        params.push(actorName)
    }
    return {
        sql: `SELECT actor_name, actor_id, COUNT(*) AS admitted, AVG(queue_wait_ms) AS average_ms, MAX(queue_wait_ms) AS max_ms
FROM request_events
WHERE ${clauses.join(" AND ")}
GROUP BY actor_name, actor_id
ORDER BY admitted DESC, actor_name, actor_id
LIMIT 500`,
        params
    }
}

export function queueWaitRows(result: ObserverQueryResult): QueueWaitRow[] {
    return result.rows.map(row => {
        const stats = { actorName: row.actor_name, actorId: row.actor_id, admitted: row.admitted, averageMs: row.average_ms, maxMs: row.max_ms }
        if (
            typeof stats.actorName !== "string" ||
            typeof stats.actorId !== "string" ||
            !Number.isSafeInteger(stats.admitted) ||
            Number(stats.admitted) <= 0 ||
            ![stats.averageMs, stats.maxMs].every(value => typeof value === "number" && Number.isFinite(value) && value >= 0)
        )
            throw new Error("Invalid queue wait row")
        return stats as QueueWaitRow
    })
}

export function queueWaitTotal(rows: QueueWaitRow[]): QueueWaitStats | null {
    return combine(rows, () => "all").get("all") ?? null
}

export function queueWaitByActor(rows: QueueWaitRow[]): Map<string, QueueWaitStats> {
    return combine(rows, row => row.actorName)
}

export function queueWaitByInstance(rows: QueueWaitRow[], actorName: string): Map<string, QueueWaitStats> {
    return combine(
        rows.filter(row => row.actorName === actorName),
        row => row.actorId
    )
}

export function formatWait(ms: number | undefined): string {
    if (ms === undefined) return "—"
    return ms >= 1000 ? `${(ms / 1000).toLocaleString(undefined, { maximumFractionDigits: 2 })} s` : `${ms.toLocaleString(undefined, { maximumFractionDigits: 1 })} ms`
}

function combine(rows: QueueWaitRow[], key: (row: QueueWaitRow) => string): Map<string, QueueWaitStats> {
    const groups = new Map<string, QueueWaitStats>()
    for (const row of rows) {
        const current = groups.get(key(row)) ?? { admitted: 0, averageMs: 0, maxMs: 0 }
        const admitted = current.admitted + row.admitted
        groups.set(key(row), {
            admitted,
            averageMs: (current.averageMs * current.admitted + row.averageMs * row.admitted) / admitted,
            maxMs: Math.max(current.maxMs, row.maxMs)
        })
    }
    return groups
}
