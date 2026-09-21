import type { ObserverQuery, ObserverQueryResult, RequestTrace, SqlValue } from "./client.js"
import { queueP95, requestSummary } from "./overview-data.js"
import type { ResolvedRange } from "./time-range.js"

export interface ClassMetrics {
    actorName: string
    count: number
    success: number | null
    p95: number | null
    queueP95: number | null
}

export interface OverviewMetrics {
    total: ClassMetrics
    classes: ClassMetrics[]
}

// Events are grouped twice: per actor class and once under '' for the deployment-wide row.
// Percentile rank is ceil(0.95 × n) in integer arithmetic, matching the live computation.
export function overviewQuery(range: ResolvedRange): ObserverQuery {
    const bounds: string[] = []
    const params: SqlValue[] = []
    if (range.fromMs !== undefined) {
        bounds.push("started_at_ms >= ?")
        params.push(range.fromMs)
    }
    if (range.toMs !== undefined) {
        bounds.push("started_at_ms <= ?")
        params.push(range.toMs)
    }
    return {
        sql: `WITH scoped AS (
    SELECT actor_name, outcome, duration_ms, queue_wait_ms FROM request_events
    ${bounds.length ? `WHERE ${bounds.join(" AND ")}` : ""}
), events AS (
    SELECT actor_name, outcome, duration_ms, queue_wait_ms FROM scoped
    UNION ALL
    SELECT '' AS actor_name, outcome, duration_ms, queue_wait_ms FROM scoped
), attempts AS (
    SELECT actor_name, outcome, duration_ms, queue_wait_ms,
        ROW_NUMBER() OVER (PARTITION BY actor_name ORDER BY duration_ms) AS duration_rank,
        COUNT(*) OVER (PARTITION BY actor_name) AS attempts,
        ROW_NUMBER() OVER (PARTITION BY actor_name ORDER BY queue_wait_ms IS NULL, queue_wait_ms) AS queue_rank,
        COUNT(queue_wait_ms) OVER (PARTITION BY actor_name) AS queued
    FROM events WHERE outcome <> 'rerouted'
), summary AS (
    SELECT actor_name, MAX(attempts) AS attempts, SUM(outcome = 'completed') AS completed,
        MAX(CASE WHEN duration_rank = MAX(1, (attempts * 95 + 99) / 100) THEN duration_ms END) AS p95_duration_ms,
        MAX(CASE WHEN queued > 0 AND queue_rank = MAX(1, (queued * 95 + 99) / 100) THEN queue_wait_ms END) AS p95_queue_wait_ms
    FROM attempts GROUP BY actor_name
)
SELECT e.actor_name, COUNT(*) AS total, COALESCE(s.attempts, 0) AS attempts, COALESCE(s.completed, 0) AS completed, s.p95_duration_ms, s.p95_queue_wait_ms
FROM events e LEFT JOIN summary s ON s.actor_name = e.actor_name
GROUP BY e.actor_name
ORDER BY e.actor_name
LIMIT 500`,
        params
    }
}

export function overviewRows(result: ObserverQueryResult): OverviewMetrics {
    const rows = result.rows.map(row => {
        const { actor_name: actorName, total, attempts, completed, p95_duration_ms: p95, p95_queue_wait_ms: queue } = row
        if (
            typeof actorName !== "string" ||
            ![total, attempts, completed].every(value => Number.isSafeInteger(value) && Number(value) >= 0) ||
            Number(attempts) > Number(total) ||
            Number(completed) > Number(attempts) ||
            ![p95, queue].every(value => value === null || value === undefined || (typeof value === "number" && Number.isFinite(value) && value >= 0))
        )
            throw new Error("Invalid overview metrics row")
        return {
            actorName,
            count: Number(total),
            success: Number(attempts) ? (100 * Number(completed)) / Number(attempts) : null,
            p95: typeof p95 === "number" ? p95 : null,
            queueP95: typeof queue === "number" ? queue : null
        }
    })
    return {
        total: rows.find(row => row.actorName === "") ?? { actorName: "", count: 0, success: null, p95: null, queueP95: null },
        classes: rows.filter(row => row.actorName !== "")
    }
}

export function liveOverviewMetrics(records: RequestTrace[]): OverviewMetrics {
    const groups = new Map<string, RequestTrace[]>()
    for (const record of records) groups.set(record.actorName, [...(groups.get(record.actorName) ?? []), record])
    const metrics = (actorName: string, traces: RequestTrace[]): ClassMetrics => ({ actorName, ...requestSummary(traces), queueP95: queueP95(traces) })
    return { total: metrics("", records), classes: [...groups].map(([actorName, traces]) => metrics(actorName, traces)) }
}
