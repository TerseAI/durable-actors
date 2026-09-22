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

export function queueWaitRows(value: unknown): QueueWaitRow[] {
    if (!Array.isArray(value) || !value.every(isQueueWait)) throw new Error("Invalid queue wait response")
    return value
}

function isQueueWait(row: QueueWaitRow): boolean {
    return (
        !!row &&
        typeof row.actorName === "string" &&
        typeof row.actorId === "string" &&
        Number.isSafeInteger(row.admitted) &&
        row.admitted > 0 &&
        [row.averageMs, row.maxMs].every(value => typeof value === "number" && Number.isFinite(value) && value >= 0) &&
        row.averageMs <= row.maxMs
    )
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
