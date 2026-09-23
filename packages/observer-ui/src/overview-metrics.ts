import type { RequestTrace } from "./client.js"
import { queueP95, requestSummary } from "./overview-data.js"

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

export function parseOverviewMetrics(value: unknown): OverviewMetrics {
    const metrics = value as OverviewMetrics | null
    if (!metrics || !isMetrics(metrics.total) || !Array.isArray(metrics.classes) || !metrics.classes.every(isMetrics)) throw new Error("Invalid overview metrics response")
    return metrics
}

function isMetrics(row: ClassMetrics): boolean {
    return (
        !!row &&
        typeof row.actorName === "string" &&
        Number.isSafeInteger(row.count) &&
        row.count >= 0 &&
        [row.success, row.p95, row.queueP95].every(value => value === null || (typeof value === "number" && Number.isFinite(value) && value >= 0)) &&
        (row.success === null || row.success <= 100)
    )
}

export function liveOverviewMetrics(records: RequestTrace[]): OverviewMetrics {
    const groups = new Map<string, RequestTrace[]>()
    for (const record of records) groups.set(record.actorName, [...(groups.get(record.actorName) ?? []), record])
    const metrics = (actorName: string, traces: RequestTrace[]): ClassMetrics => ({ actorName, ...requestSummary(traces), queueP95: queueP95(traces) })
    return { total: metrics("", records), classes: [...groups].map(([actorName, traces]) => metrics(actorName, traces)) }
}
