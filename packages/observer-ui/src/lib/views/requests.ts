import type { ApplyEvents } from "../../hooks/useLiveView.js"
import type { RequestTrace } from "../../types/Requests.js"

export type RequestFilter = { actorName?: string; actorId?: string }

/** Adds streamed requests that match the filter, dropping those older than the live window. */
export function applyRequestEvents(durationMs: number, { actorName, actorId }: RequestFilter = {}): ApplyEvents<RequestTrace[]> {
    return (traces, events) => {
        const added: RequestTrace[] = []
        for (const event of events) {
            if (event.type !== "request") continue
            const trace = event.item
            if ((actorName === undefined || trace.actorName === actorName) && (actorId === undefined || trace.actorId === actorId)) added.push(trace)
        }
        if (added.length === 0) return traces
        const since = Date.now() - durationMs
        return [...new Map([...traces, ...added].map(trace => [trace.eventId, trace])).values()].filter(trace => trace.startedAtMs >= since)
    }
}
