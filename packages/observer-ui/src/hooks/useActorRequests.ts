import { useData } from "../client/DatatContext.js"
import { useTimeWindow } from "../client/TimeRangeContext.js"
import { requestsKey } from "../client/cacheKeys.js"
import { HttpObserverClient } from "../client/observerClient.js"
import { currentBounds, windowKey } from "../lib/timeRange.js"
import { type RequestFilter, applyRequestEvents } from "../lib/views/requests.js"

import { useLiveView } from "./useLiveView.js"

// The shared request sample is capped project-wide, so busy actors can fall out of it.
// Actor pages read their own history.
export function useActorTypeRequests(actorName: string) {
    return useFilteredRequests({ actorName })
}

export function useInstanceRequests(actorName: string, actorId: string) {
    return useFilteredRequests({ actorName, actorId })
}

function useFilteredRequests(filter: RequestFilter) {
    const { baseUrl, events } = useData()
    const timeWindow = useTimeWindow()
    const { data, error, isLoading } = useLiveView(
        [...requestsKey(baseUrl), filter.actorName, filter.actorId, ...windowKey(timeWindow)],
        () => new HttpObserverClient(baseUrl).collectRequests(currentBounds(timeWindow), filter),
        applyRequestEvents(timeWindow.toMs - timeWindow.fromMs, filter),
        timeWindow.live ? events : null
    )
    return {
        requests: data?.filter(trace => trace.startedAtMs >= timeWindow.fromMs) ?? null,
        isLoading,
        isError: !data && Boolean(error),
        error
    }
}
