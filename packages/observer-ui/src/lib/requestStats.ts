import type { RequestTrace } from "../types/Requests.js"
import type { TimeWindow } from "../types/TimeRange.js"

import { bucketCount, bucketIndex } from "./timeRange.js"

export type TimingStats = {
    average: number
    p95: number
    max: number
    series: number[]
}

export type TimingPick = (trace: RequestTrace) => number | null

export const queueTime: TimingPick = trace => trace.queueWaitMs

// durationMs includes the queue wait, so latency excluding the queue subtracts it.
export const serviceTime: TimingPick = trace => trace.durationMs - (trace.queueWaitMs ?? 0)

export function timingStats(requests: RequestTrace[], timeWindow: TimeWindow, pick: TimingPick): TimingStats {
    const values = picked(requests, pick)
    return {
        average: average(values),
        p95: percentile(values, 0.95),
        max: values.length ? Math.max(...values) : 0,
        series: bucketAverages(requests, timeWindow, pick)
    }
}

export function averageOf(requests: RequestTrace[], pick: TimingPick): number | null {
    const values = picked(requests, pick)
    return values.length ? average(values) : null
}

export function average(values: number[]): number {
    return values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : 0
}

function percentile(values: number[], rank: number): number {
    if (!values.length) return 0
    const sorted = [...values].sort((a, b) => a - b)
    return sorted[Math.min(sorted.length - 1, Math.ceil(rank * sorted.length) - 1)]!
}

function picked(requests: RequestTrace[], pick: TimingPick): number[] {
    return requests.map(pick).filter((value): value is number => value !== null)
}

function bucketAverages(requests: RequestTrace[], timeWindow: TimeWindow, pick: TimingPick): number[] {
    const buckets = Array.from({ length: bucketCount(timeWindow) }, () => [] as number[])
    for (const trace of requests) {
        const index = bucketIndex(timeWindow, trace.startedAtMs)
        const value = pick(trace)
        if (index !== null && value !== null) buckets[index]!.push(value)
    }
    return buckets.map(average)
}
