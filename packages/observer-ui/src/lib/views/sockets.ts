import type { ObserveEvent } from "../../types/Events.js"
import type { SocketSession } from "../../types/Sockets.js"

import { gatewayStaleAfterMs } from "./actors.js"

// The newest sessions the server returns; older ones fall off as new ones open.
const sessionLimit = 500

export function applySocketEvents(sessions: SocketSession[], events: ObserveEvent[]): SocketSession[] {
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
export function currentSessions(sessions: SocketSession[], gateways: Record<string, number>, nowMs: number): SocketSession[] {
    return sessions.map(session => {
        if (session.status !== "open") return session
        const seen = Math.max(gateways[session.gatewayId] ?? 0, session.openedAtMs)
        return seen + gatewayStaleAfterMs >= nowMs ? session : { ...session, status: "lost", closedAtMs: seen }
    })
}
