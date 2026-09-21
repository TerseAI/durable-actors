import type { ObserverClient } from "./client.js"
import { usePolledQuery } from "./observer-hooks.js"
import { queueWaitQuery, queueWaitRows } from "./queue-wait.js"
import type { ResolvedRange } from "./time-range.js"

export function useQueueWaits(client: Pick<ObserverClient, "query">, range: ResolvedRange, actorName?: string) {
    const polled = usePolledQuery(client, queueWaitQuery(range, actorName), queueWaitRows)
    return { supported: polled.supported, rows: polled.value, failed: polled.failed }
}
