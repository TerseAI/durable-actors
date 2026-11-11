import { requestsKey } from "../client/cacheKeys.js"
import type { EventBus } from "../client/events.js"
import { HttpObserverClient } from "../client/observerClient.js"
import { currentBounds, windowKey } from "../lib/timeRange.js"
import { applyRequestEvents } from "../lib/views/requests.js"
import type { TimeWindow } from "../types/TimeRange.js"

import { useLiveView } from "./useLiveView.js"

export function useRequests(baseUrl: string, timeWindow: TimeWindow, events: EventBus | null) {
    const { data, error, isLoading } = useLiveView(
        [...requestsKey(baseUrl), ...windowKey(timeWindow)],
        () => new HttpObserverClient(baseUrl).collectRequests(currentBounds(timeWindow)),
        applyRequestEvents(timeWindow.toMs - timeWindow.fromMs),
        timeWindow.live ? events : null
    )
    return {
        requests: data ?? null,
        isLoading,
        isError: !data && Boolean(error),
        error
    }
}
