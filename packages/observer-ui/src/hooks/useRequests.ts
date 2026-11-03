import useSWRSubscription, { type SWRSubscriptionOptions } from "swr/subscription"

import { HttpObserverClient } from "../client/observerClient.js"
import { subscribeToRequests } from "../client/streams/requestStream.js"
import type { RequestTrace } from "../types/Requests.js"
import type { TimeWindow } from "../types/TimeRange.js"

import { useWindowedSWR } from "./useWindowedSWR.js"

export function useRequests(timeWindow: TimeWindow) {
    const { live, fromMs, toMs } = timeWindow
    const durationMs = toMs - fromMs
    const { data, error, isLoading } = useWindowedSWR(["requests"], timeWindow, async bounds => (await new HttpObserverClient().listRequests(bounds)).records)
    
    const { data: streamed } = useSWRSubscription(live ? ["requests/events", durationMs] : null, (_key, { next }: SWRSubscriptionOptions<RequestTrace[]>) =>
        subscribeToRequests(page => next(null, traces => mergeTraces(traces ?? [], page.records, durationMs)))
    )

    return {
        requests: streamed ?? data ?? null,
        isLoading,
        isError: !streamed && Boolean(error),
        error
    }
}

// The stream sends incremental pages rather than full snapshots, so records accumulate until they leave the window.
function mergeTraces(traces: RequestTrace[], incoming: RequestTrace[], durationMs: number): RequestTrace[] {
    const since = Date.now() - durationMs
    const byEvent = new Map([...traces, ...incoming].map(trace => [trace.eventId, trace]))
    return [...byEvent.values()].filter(trace => trace.startedAtMs >= since)
}
