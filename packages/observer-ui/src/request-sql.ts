import { isTrace } from "./client.js"
import type { ObserverQuery, ObserverQueryResult, RequestTrace, RequestTracePage, SqlValue } from "./client.js"

export interface HistoryFilters {
    fromMs?: number
    toMs?: number
    actorName?: string
    actorId?: string
    outcome?: RequestTrace["outcome"]
}
interface HistoryCursor {
    watermark: number
    sequence: number
    time: number
    generation: string
    pruned: number
}
const pageSize = 100

export function historyQuery(filters: HistoryFilters, cursor?: string): ObserverQuery {
    const clauses: string[] = []
    const params: SqlValue[] = []
    for (const [column, operator, value] of [
        ["started_at_ms", ">=", filters.fromMs],
        ["started_at_ms", "<=", filters.toMs],
        ["actor_name", "=", filters.actorName],
        ["actor_id", "=", filters.actorId],
        ["outcome", "=", filters.outcome]
    ] as const) {
        if (value !== undefined) {
            clauses.push(`${column} ${operator} ?`)
            params.push(value)
        }
    }
    if (cursor) {
        const position: HistoryCursor = JSON.parse(cursor)
        clauses.push("sequence <= ?", "(started_at_ms, sequence) < (?, ?)")
        params.push(position.watermark, position.time, position.sequence)
    }
    return {
        sql: `SELECT h.generation, h.watermark, h.pruned, h.total, r.sequence, r.event
FROM request_history h LEFT JOIN (
    SELECT sequence, event, started_at_ms FROM request_events
    ${clauses.length ? `WHERE ${clauses.join(" AND ")}` : ""}
    ORDER BY started_at_ms DESC, sequence DESC LIMIT ${pageSize + 1}
) r ON 1 = 1
ORDER BY r.started_at_ms DESC, r.sequence DESC`,
        params
    }
}

export function historyPage(result: ObserverQueryResult, cursor?: string): RequestTracePage {
    if (result.truncated) throw new Error("History query exceeded the server result limit")
    const metadata = result.rows[0]
    if (!metadata || typeof metadata.generation !== "string" || ![metadata.watermark, metadata.pruned, metadata.total].every(value => Number.isSafeInteger(value) && Number(value) >= 0))
        throw new Error("Invalid history metadata")
    const previous: HistoryCursor | undefined = cursor ? JSON.parse(cursor) : undefined
    const records = result.rows
        .filter(row => row.sequence !== null)
        .map(row => {
            if (typeof row.event !== "string") throw new Error("Invalid saved event")
            const record: unknown = { ...JSON.parse(row.event), sequence: row.sequence }
            if (!isTrace(record)) throw new Error("Invalid saved event")
            return record
        })
    const more = records.length > pageSize
    records.length = Math.min(records.length, pageSize)
    const last = records.at(-1)
    const watermark = previous?.watermark ?? Number(metadata.watermark)
    return {
        epoch: metadata.generation,
        cursor: Number(metadata.watermark),
        capacity: pageSize,
        dropped: 0,
        evicted: 0,
        records,
        reset: !!previous && (previous.generation !== metadata.generation || previous.pruned < Number(metadata.pruned)),
        nextCursor:
            more && last
                ? JSON.stringify({ watermark, sequence: last.sequence, time: last.startedAtMs, generation: metadata.generation, pruned: Number(metadata.pruned) } satisfies HistoryCursor)
                : null
    }
}
