import type { ActorInventory, ObserverQuery, ObserverQueryResult } from "./client.js"

export type SocketSessionStatus = "open" | "closed" | "lost"

export interface SocketSession {
    connectionId: string
    actorName: string
    actorId: string
    hostId: string | null
    openedAtMs: number | null
    closedAtMs: number | null
    lastSeenMs: number | null
    messages: number
    failures: number
    status: SocketSessionStatus
    metadata?: unknown
}

export type SocketSessionRow = Omit<SocketSession, "status" | "metadata">

export interface SocketDuration {
    ms: number
    lowerBound: boolean
}

export const sessionLimit = 500

export function sessionsQuery(fromMs?: number): ObserverQuery {
    return {
        sql: `SELECT connection_id, actor_name, actor_id, MIN(host_id) AS host_id,
    MIN(CASE WHEN operation = 'onConnect' THEN started_at_ms END) AS opened_at_ms,
    MAX(CASE WHEN operation = 'onDisconnect' THEN started_at_ms END) AS closed_at_ms,
    MAX(started_at_ms) AS last_seen_ms,
    SUM(operation = 'onMessage') AS messages,
    SUM(outcome NOT IN ('completed', 'rerouted')) AS failures
FROM request_events
WHERE kind = 'websocket' AND connection_id IS NOT NULL
GROUP BY connection_id, actor_name, actor_id
${fromMs === undefined ? "" : "HAVING MAX(started_at_ms) >= ?"}
ORDER BY COALESCE(opened_at_ms, last_seen_ms) DESC, connection_id
LIMIT ${sessionLimit}`,
        params: fromMs === undefined ? [] : [fromMs]
    }
}

export function sessionRows(result: ObserverQueryResult): SocketSessionRow[] {
    return result.rows.map(row => {
        const session = {
            connectionId: row.connection_id,
            actorName: row.actor_name,
            actorId: row.actor_id,
            hostId: row.host_id ?? null,
            openedAtMs: row.opened_at_ms ?? null,
            closedAtMs: row.closed_at_ms ?? null,
            lastSeenMs: row.last_seen_ms ?? null,
            messages: row.messages ?? 0,
            failures: row.failures ?? 0
        }
        if (!isSessionRow(session)) throw new Error("Invalid WebSocket session row")
        return session
    })
}

export function socketSessions(rows: SocketSessionRow[], inventory: ActorInventory | undefined): SocketSession[] {
    const live = new Map<string, unknown>()
    for (const actor of inventory?.actors ?? [])
        for (const instance of actor.instances) for (const connection of instance.connections) live.set(JSON.stringify([actor.actorName, instance.actorId, connection.id]), connection.metadata)
    const sessions: SocketSession[] = rows.map(row => {
        const key = JSON.stringify([row.actorName, row.actorId, row.connectionId])
        const status: SocketSessionStatus = row.closedAtMs !== null ? "closed" : live.has(key) ? "open" : "lost"
        return status === "open" ? { ...row, status, metadata: live.get(key) } : { ...row, status }
    })
    const known = new Set(sessions.map(session => JSON.stringify([session.actorName, session.actorId, session.connectionId])))
    for (const actor of inventory?.actors ?? [])
        for (const instance of actor.instances)
            for (const connection of instance.connections) {
                if (known.has(JSON.stringify([actor.actorName, instance.actorId, connection.id]))) continue
                sessions.unshift({
                    connectionId: connection.id,
                    actorName: actor.actorName,
                    actorId: instance.actorId,
                    hostId: null,
                    openedAtMs: null,
                    closedAtMs: null,
                    lastSeenMs: null,
                    messages: 0,
                    failures: 0,
                    status: "open",
                    metadata: connection.metadata
                })
            }
    return sessions
}

export function sessionDuration(session: Pick<SocketSession, "status" | "openedAtMs" | "closedAtMs" | "lastSeenMs">, now: number): SocketDuration | null {
    if (session.openedAtMs === null) return null
    const end = session.status === "closed" ? session.closedAtMs! : session.status === "open" ? Math.max(now, session.openedAtMs) : session.lastSeenMs!
    return { ms: Math.max(0, end - session.openedAtMs), lowerBound: session.status === "lost" }
}

export function sessionSummary(sessions: SocketSession[], now: number) {
    const durations = sessions.map(session => sessionDuration(session, now)).filter((duration): duration is SocketDuration => duration !== null && !duration.lowerBound)
    const messages = sessions.reduce((sum, session) => sum + session.messages, 0)
    return {
        total: sessions.length,
        open: sessions.filter(session => session.status === "open").length,
        lost: sessions.filter(session => session.status === "lost").length,
        median: percentile(
            durations.map(duration => duration.ms),
            0.5
        ),
        p95: percentile(
            durations.map(duration => duration.ms),
            0.95
        ),
        messages,
        messagesPerSession: sessions.length ? messages / sessions.length : null
    }
}

export function formatDuration(ms: number): string {
    if (ms < 1000) return `${ms.toLocaleString(undefined, { maximumFractionDigits: 0 })} ms`
    if (ms < 60_000) return `${(ms / 1000).toLocaleString(undefined, { maximumFractionDigits: 1 })} s`
    const minutes = Math.floor(ms / 60_000)
    if (minutes < 60) return `${minutes}m ${Math.floor((ms % 60_000) / 1000)}s`
    const hours = Math.floor(minutes / 60)
    if (hours < 24) return `${hours}h ${minutes % 60}m`
    return `${Math.floor(hours / 24)}d ${hours % 24}h`
}

function percentile(values: number[], fraction: number) {
    if (!values.length) return null
    return values.sort((a, b) => a - b)[Math.max(0, Math.ceil(values.length * fraction) - 1)]!
}

function isSessionRow(value: Record<string, unknown>): value is SocketSessionRow & Record<string, unknown> {
    return (
        typeof value.connectionId === "string" &&
        typeof value.actorName === "string" &&
        typeof value.actorId === "string" &&
        (value.hostId === null || typeof value.hostId === "string") &&
        [value.openedAtMs, value.closedAtMs, value.lastSeenMs].every(time => time === null || (Number.isSafeInteger(time) && Number(time) >= 0)) &&
        [value.messages, value.failures].every(count => Number.isSafeInteger(count) && Number(count) >= 0) &&
        (value.openedAtMs !== null || value.lastSeenMs !== null)
    )
}
