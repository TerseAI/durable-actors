import type { ObserverClient } from "./client.js"
import { usePolledQuery } from "./observer-hooks.js"
import type { ResolvedRange } from "./time-range.js"

export function useQueueWaits(client: Pick<ObserverClient, "listQueueWaits">, range: ResolvedRange, actorName?: string) {
    const polled = usePolledQuery(client, { ...range, actorName }, client.listQueueWaits)
    return { supported: polled.supported, rows: polled.value, failed: polled.failed }
}
