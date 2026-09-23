import type { ObserverClient } from "./client.js"
import { usePolledQuery } from "./observer-hooks.js"
import { sessionLimit } from "./socket-sessions.js"
import type { ResolvedRange } from "./time-range.js"

export function useSocketHistory(client: Pick<ObserverClient, "listWebSockets">, range: ResolvedRange) {
    const polled = usePolledQuery(client, range, client.listWebSockets, 5_000)
    return { ...polled, rows: polled.value, capped: (polled.value?.length ?? 0) >= sessionLimit }
}
