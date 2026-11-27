import type { ObserveEvent } from "../../types/Events.js"
import type { SocketSession } from "../../types/Sockets.js"

const gatewayStaleAfterMs = 30_000

// The newest sessions the server returns; older ones fall off as new ones open.
const sessionLimit = 500

type SocketHistory = { sessions: SocketSession[]; gateways: Record<string, number> }

export function applySocketEvents(history: SocketHistory, events: ObserveEvent[]): SocketHistory {
    const connections = events.filter(event => event.type === "connectionOpened" || event.type === "connectionClosed")
    const heartbeats = events.filter(event => event.type === "gatewayAlive")
    if (!connections.length && !heartbeats.length) return history
    const sessions = connections.length ? updateSessions(history.sessions, connections) : history.sessions
    const openGateways = new Set(sessions.filter(session => session.status === "open").map(session => session.gatewayId))
    const gateways = Object.fromEntries(Object.entries(history.gateways).filter(([id]) => openGateways.has(id)))
    for (const {
        item: { gatewayId, atMs }
    } of heartbeats) {
        if (openGateways.has(gatewayId)) gateways[gatewayId] = Math.max(gateways[gatewayId] ?? 0, atMs)
    }
    return { sessions, gateways }
}

function updateSessions(sessions: SocketSession[], events: ObserveEvent[]): SocketSession[] {
    const byId = new Map(sessions.map(session => [session.connectionId, session]))
    for (const event of events) {
        if (event.type === "connectionOpened" && !byId.has(event.item.connectionId)) {
            const { connectionId, actorName, actorId, gatewayId, openedAtMs, metadata } = event.item
            byId.set(connectionId, { connectionId, actorName, actorId, gatewayId, status: "open", openedAtMs, closedAtMs: null, metadata })
        }
        if (event.type === "connectionClosed") {
            const { connectionId, actorName, actorId, gatewayId, openedAtMs, closedAtMs, closeCode, closeReason, messagesIn, messagesOut } = event.item
            const opened = byId.get(connectionId)
            byId.set(connectionId, {
                connectionId,
                actorName,
                actorId,
                gatewayId,
                metadata: opened?.metadata,
                status: "closed",
                openedAtMs: opened?.openedAtMs ?? openedAtMs,
                closedAtMs,
                closeCode: closeCode ?? undefined,
                closeReason: closeReason ?? undefined,
                messagesIn: messagesIn ?? undefined,
                messagesOut: messagesOut ?? undefined
            })
        }
    }
    return [...byId.values()].sort((a, b) => b.openedAtMs - a.openedAtMs).slice(0, sessionLimit)
}

/** Sessions as of `nowMs`: an open session whose gateway has gone silent is lost when it last reported. */
export function currentSessions(sessions: SocketSession[], gateways: Record<string, number>, nowMs: number, liveGateways?: Record<string, number>): SocketSession[] {
    return sessions.map(session => {
        if (session.status !== "open") return session
        const seen = Math.max(gateways[session.gatewayId] ?? 0, liveGateways?.[session.gatewayId] ?? 0, session.openedAtMs)
        return seen + gatewayStaleAfterMs >= nowMs ? session : { ...session, status: "lost", closedAtMs: seen }
    })
}
