import type { TimeWindow } from "../types/TimeRange.js"

import { bucketCount, bucketIndex } from "./timeRange.js"

/** The highest value in each bucket of the window. Empty buckets are 0, or repeat the previous value with `carry`. */
export function bucketMax<T extends { atMs: number }>(points: T[], timeWindow: TimeWindow, value: (point: T) => number | null, carry = false): number[] {
    const series = Array<number | null>(bucketCount(timeWindow)).fill(null)
    for (const point of points) {
        const index = bucketIndex(timeWindow, point.atMs)
        const current = value(point)
        if (index !== null && current !== null) series[index] = Math.max(series[index] ?? 0, current)
    }
    let previous = 0
    return series.map(entry => {
        if (entry !== null) previous = entry
        return entry ?? (carry ? previous : 0)
    })
}
