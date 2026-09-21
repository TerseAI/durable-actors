import type { ObserverClient } from "./client.js"
import { usePolledQuery } from "./observer-hooks.js"
import { sessionLimit, sessionRows, sessionsQuery } from "./socket-sessions.js"
import type { ResolvedRange } from "./time-range.js"

export function useSocketHistory(client: Pick<ObserverClient, "query">, range: ResolvedRange) {
    const polled = usePolledQuery(client, sessionsQuery(range), sessionRows, 5_000)
    return { ...polled, rows: polled.value, capped: (polled.value?.length ?? 0) >= sessionLimit }
}
