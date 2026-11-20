import { formatTick } from "../../format.js"
import type { Histogram } from "../../types/Histogram.js"
import type { TimeBounds } from "../../types/TimeRange.js"

/** Counts by bucket start, plus the time the store was read: later counts are added here. */
export interface LiveHistogram<T> {
    bucketMs: number
    readAtMs: number
    counts: Record<number, T>
}

/** Counts the live stream sends for one second. */
export type SecondCounts<T> = { atMs: number; counts: T }

export function liveHistogram<T>({ fromMs, bucketMs, buckets }: Histogram<T>, readAtMs: number): LiveHistogram<T> {
    const counts: Record<number, T> = {}
    buckets.forEach((bucket, index) => (counts[fromMs + index * bucketMs] = bucket))
    return { bucketMs, readAtMs, counts }
}

// Each second counts once: the read covered the seconds before readAtMs, the stream adds the rest.
export function addCounts<T>(histogram: LiveHistogram<T>, seconds: SecondCounts<T>[], add: (total: T | undefined, more: T) => T): LiveHistogram<T> {
    const later = seconds.filter(second => second.atMs >= histogram.readAtMs)
    if (!later.length) return histogram
    const counts = { ...histogram.counts }
    for (const { atMs, counts: more } of later) {
        const start = Math.floor(atMs / histogram.bucketMs) * histogram.bucketMs
        counts[start] = add(counts[start], more)
    }
    return { ...histogram, counts }
}

/** The histogram drawn for aligned bounds, which move with a live window. */
export function histogramFor<T>({ bucketMs, counts }: LiveHistogram<T>, { fromMs, toMs }: TimeBounds, empty: () => T): Histogram<T> {
    const buckets: T[] = []
    for (let start = fromMs; start < toMs; start += bucketMs) buckets.push(counts[start] ?? empty())
    return { fromMs, bucketMs, buckets }
}

export function bucketEndLabel<T>({ fromMs, bucketMs, buckets }: Histogram<T>, index: number): string {
    return formatTick(fromMs + (index + 1) * bucketMs, bucketMs * buckets.length)
}
