export interface ActorLog {
    eventId: string
    timestampMs: number
    stream: "stdout" | "stderr"
    level: string
    message: string
    fields: Record<string, unknown>
    requestId?: string | null
    projectId: string
    actorName: string
    actorId: string
    hostId: string
    sessionId: string
}

export interface LogQuery {
    actorName?: string
    actorId?: string
    requestId?: string
    level?: string
    search?: string
    fromMs?: number
    toMs?: number
    limit?: number
    cursor?: string
}

export interface LogPage {
    records: ActorLog[]
    nextCursor: string | null
    dropped: number
    persistenceFailed: boolean
}

export const displayedLogLimit = 500

export function mergeLogs(current: ActorLog[], incoming: ActorLog[]): ActorLog[] {
    const records = new Map(current.map(record => [record.eventId, record]))
    for (const record of incoming) records.set(record.eventId, record)
    return [...records.values()].sort((a, b) => b.timestampMs - a.timestampMs || b.eventId.localeCompare(a.eventId)).slice(0, displayedLogLimit)
}

export function isLogPage(value: unknown): value is LogPage {
    if (!value || typeof value !== "object") return false
    const page = value as LogPage
    return (
        Array.isArray(page.records) &&
        page.records.length <= displayedLogLimit &&
        page.records.every(isLog) &&
        (page.nextCursor === null || typeof page.nextCursor === "string") &&
        Number.isSafeInteger(page.dropped) &&
        page.dropped >= 0 &&
        typeof page.persistenceFailed === "boolean"
    )
}

function isLog(value: unknown): value is ActorLog {
    if (!value || typeof value !== "object") return false
    const log = value as ActorLog
    return (
        [log.eventId, log.level, log.message, log.projectId, log.actorName, log.actorId, log.hostId, log.sessionId].every(value => typeof value === "string") &&
        log.eventId.length > 0 &&
        Number.isSafeInteger(log.timestampMs) &&
        log.timestampMs >= 0 &&
        log.timestampMs <= 8.64e15 &&
        ["stdout", "stderr"].includes(log.stream) &&
        !!log.fields &&
        typeof log.fields === "object" &&
        !Array.isArray(log.fields) &&
        (log.requestId == null || typeof log.requestId === "string")
    )
}
