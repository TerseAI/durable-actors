import type { SocketSession } from "../types/Sockets.js"
import type { TimeSpan } from "../types/TimeRange.js"

export function sessionSpan(session: SocketSession): TimeSpan {
    return { fromMs: session.openedAtMs, toMs: session.closedAtMs ?? Infinity }
}
