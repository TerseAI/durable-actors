import useSWR from "swr"
import useSWRSubscription, { type SWRSubscriptionOptions } from "swr/subscription"
import type { RequestTrace } from "../types/Requests.js"
import { HttpObserverClient } from "../client/observerClient.js"
import { subscribeToRequests } from "../client/requestStream.js"
import { trendWindowMs } from "../lib/trendWindow.js"

export function useRequests() {
    const { data, error, isLoading } = useSWR<RequestTrace[]>(
        "requests",
        async () => (await new HttpObserverClient().listRequests(Date.now() - trendWindowMs)).records,
    )
    const { data: live } = useSWRSubscription(
        "requests/events",
        (_key: string, { next }: SWRSubscriptionOptions<RequestTrace[]>) =>
            subscribeToRequests(page => next(null, traces => mergeTraces(traces ?? [], page.records))),
    )

    return {
        requests: live ?? data ?? null,
        isLoading,
        isError: !live && Boolean(error),
        error,
    }
}

// The stream sends incremental pages rather than full snapshots, so records accumulate until they leave the window.
function mergeTraces(traces: RequestTrace[], incoming: RequestTrace[]): RequestTrace[] {
    const since = Date.now() - trendWindowMs
    const byEvent = new Map([...traces, ...incoming].map(trace => [trace.eventId, trace]))
    return [...byEvent.values()].filter(trace => trace.startedAtMs >= since)
}
