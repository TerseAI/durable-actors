import type { EventBus } from "../client/events.js"
import { alignBounds } from "../lib/logs.js"
import { currentBounds, windowKey } from "../lib/timeRange.js"
import { type LiveHistogram, histogramFor, liveHistogram } from "../lib/views/histogram.js"
import type { Histogram } from "../types/Histogram.js"
import type { TimeBounds, TimeWindow } from "../types/TimeRange.js"

import { type ApplyEvents, useLiveView } from "./useLiveView.js"

export type HistogramSource<T> = {
    key: readonly unknown[]
    read: (bounds: TimeBounds, bucketMs: number) => Promise<Histogram<T>>
    apply: ApplyEvents<LiveHistogram<T>>
    empty: () => T
}

/** A histogram read from the store, with the stream's records counted in while the window is live. */
export function useLiveHistogram<T>({ key, read, apply, empty }: HistogramSource<T>, timeWindow: TimeWindow, bucketMs: number, events: EventBus | null) {
    const { data, error } = useLiveView(
        [...key, ...windowKey(timeWindow), bucketMs],
        async () => {
            const readAtMs = Date.now()
            return liveHistogram(await read(alignBounds(currentBounds(timeWindow), bucketMs), bucketMs), readAtMs)
        },
        apply,
        timeWindow.live ? events : null,
        { keepPreviousData: true }
    )
    return {
        histogram: data && histogramFor(data, alignBounds(currentBounds(timeWindow), bucketMs), empty),
        isError: !data && Boolean(error)
    }
}
