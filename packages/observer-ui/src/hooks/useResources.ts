import { useData } from "../client/DatatContext.js"
import { resourcesKey } from "../client/cacheKeys.js"
import { HttpObserverClient } from "../client/observerClient.js"
import type { TimeWindow } from "../types/TimeRange.js"

import { useWindowedSWR } from "./useWindowedSWR.js"

// Usage is sampled every 5 seconds and has no event stream, so live windows poll at that pace.
const liveRefreshMs = 5_000

export function useResources(actorName: string, actorId: string, timeWindow: TimeWindow) {
    const { baseUrl } = useData()
    const { data, error } = useWindowedSWR(resourcesKey(baseUrl, actorName, actorId), timeWindow, bounds => new HttpObserverClient(baseUrl).getResources(actorName, actorId, bounds), {
        refreshInterval: timeWindow.live ? liveRefreshMs : 0
    })

    return {
        resources: data ?? null,
        isError: !data && Boolean(error)
    }
}
