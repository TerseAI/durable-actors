import { useEffect } from "react"

import useSWRInfinite from "swr/infinite"

import { entriesKey, entryHistogramKey, streamedEntriesKey } from "../client/cacheKeys.js"
import { HttpObserverClient } from "../client/observerClient.js"
import { type Entry, addCounts, countedBy, lifecycleRecords, newestFirst, shownBy, toEntry } from "../lib/entries.js"
import { alignBounds, chartBucketMs } from "../lib/logs.js"
import { requiredValue } from "../lib/query.js"
import { currentBounds, windowKey } from "../lib/timeRange.js"
import { applyEntryCounts, applyEntryEvents, entryHistogramFor, liveEntryHistogram } from "../lib/views/entries.js"
import type { EntryFilter, EntryPage } from "../types/Entries.js"
import type { TimeWindow } from "../types/TimeRange.js"

import { useEventStream, useLiveView } from "./useLiveView.js"
import { useResidency } from "./useResidency.js"

/** A class's or instance's requests, console lines, connections, lifecycle and kills, newest first, with their histogram. */
export function useEntries(baseUrl: string, timeWindow: TimeWindow, filter: EntryFilter) {
    const events = useEventStream(baseUrl, timeWindow.live ? streamQuery(filter) : null)
    const history = useEntryHistory(baseUrl, timeWindow, filter)
    // A reconnect rereads history before it drops streamed rows, so the gap's records appear and none vanish.
    const { data: streamed } = useLiveView(
        streamedEntriesKey(baseUrl, filter),
        async (): Promise<Entry[]> => {
            if (history.data) await history.mutate()
            return []
        },
        applyEntryEvents(filter),
        events
    )
    const lifecycle = useLifecycle(timeWindow, filter)
    const histogram = useEntryHistogram(baseUrl, timeWindow, filter, events)
    const pages = history.data
    const last = pages?.at(-1)
    const cutoffMs = last?.cutoffMs ?? null
    const { setSize } = history
    // A page can come back empty when a source stops it short; the list would have nothing to scroll.
    useEffect(() => {
        if (pages && last?.nextCursor && last.records.length === 0) void setSize(pages.length + 1)
    }, [pages, last, setSize])
    const loaded = pages ? [...(streamed ?? []), ...pages.flatMap(page => page.records.map(toEntry)), ...lifecycle] : null
    return {
        entries: loaded && newestFirst(loaded.filter(entry => shownBy(entry, filter) && (cutoffMs === null || entry.atMs >= cutoffMs))),
        hasOlder: Boolean(last?.nextCursor),
        loadingOlder: Boolean(pages && history.size > pages.length && (!history.error || history.isValidating)),
        // After a failed page, asking for one past the loaded pages retries it instead of skipping ahead.
        loadOlder: () => setSize((pages?.length ?? 0) + 1),
        histogram:
            histogram.data &&
            addCounts(
                histogram.data,
                lifecycle.filter(entry => countedBy(entry, filter))
            ),
        isError: (!pages && Boolean(history.error)) || (!histogram.data && Boolean(histogram.error))
    }
}

// The server narrows every channel to the actor and logs to a required request; the views apply the rest.
function streamQuery({ actorName, actorId, query }: EntryFilter): URLSearchParams {
    const params = new URLSearchParams({ signals: "requests,logs,platform", actorName })
    const instance = actorId ?? requiredValue(query, "instance")
    if (instance) params.set("actorId", instance)
    const requestId = requiredValue(query, "request")
    if (requestId) params.set("requestId", requestId)
    return params
}

function useEntryHistory(baseUrl: string, timeWindow: TimeWindow, filter: EntryFilter) {
    const client = new HttpObserverClient(baseUrl)
    return useSWRInfinite<EntryPage>(
        (index, previous: EntryPage | null) => (previous && !previous.nextCursor ? null : [...entriesKey(baseUrl, filter), ...windowKey(timeWindow), index === 0 ? null : previous!.nextCursor]),
        (key: readonly unknown[]) => client.listEntries(filter, currentBounds(timeWindow), key.at(-1) as string | null),
        { revalidateFirstPage: false, revalidateOnFocus: false }
    )
}

// Residency already folds lifecycle reports into live and dormant periods, live updates included.
function useLifecycle(timeWindow: TimeWindow, filter: EntryFilter): Entry[] {
    const { residency } = useResidency(filter.actorName, timeWindow, filter.actorId ?? requiredValue(filter.query, "instance") ?? undefined)
    return residency ? lifecycleRecords(filter.actorName, residency.periods, timeWindow.fromMs).map(toEntry) : []
}

function useEntryHistogram(baseUrl: string, timeWindow: TimeWindow, filter: EntryFilter, events: ReturnType<typeof useEventStream>) {
    const bucketMs = chartBucketMs(timeWindow.toMs - timeWindow.fromMs)
    const { data, error } = useLiveView(
        [...entryHistogramKey(baseUrl, filter), ...windowKey(timeWindow), bucketMs],
        async () => {
            const readAtMs = Date.now()
            const read = await new HttpObserverClient(baseUrl).getEntryHistogram(filter, alignBounds(currentBounds(timeWindow), bucketMs), bucketMs)
            return liveEntryHistogram(read, readAtMs)
        },
        applyEntryCounts(filter),
        events
    )
    return { data: data && entryHistogramFor(data, alignBounds(currentBounds(timeWindow), bucketMs)), error }
}
