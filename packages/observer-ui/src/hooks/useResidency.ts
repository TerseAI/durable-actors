import { HttpObserverClient } from "../client/observerClient.js"
import type { TimeWindow } from "../types/TimeRange.js"

import { useWindowedSWR } from "./useWindowedSWR.js"

// Residency has no event stream, so live windows poll instead.
const liveRefreshMs = 15_000

export function useResidency(actorName: string, timeWindow: TimeWindow) {
    const { data, error, isLoading } = useWindowedSWR(["residency", actorName], timeWindow, bounds => new HttpObserverClient().listResidency(actorName, bounds), {
        refreshInterval: timeWindow.live ? liveRefreshMs : 0
    })

    return {
        residency: data ?? null,
        isLoading,
        isError: !data && Boolean(error),
        error
    }
}
