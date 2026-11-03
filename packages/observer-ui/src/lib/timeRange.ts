import type { TimeRange, TimeWindow } from "../types/TimeRange.js"

const second = 1000
const minute = 60 * second
const hour = 60 * minute
const targetBuckets = 30
const niceSteps = [10 * second, 30 * second, minute, 2 * minute, 5 * minute, 15 * minute, 30 * minute, hour, 3 * hour, 6 * hour, 12 * hour, 24 * hour]

export const lastHour: TimeRange = { mode: "live", durationMs: hour }

export function resolveWindow(range: TimeRange, now: number): TimeWindow {
    const [fromMs, toMs] = range.mode === "live" ? [now - range.durationMs, now] : [range.fromMs, range.toMs]
    return { fromMs, toMs, stepMs: pickStep(toMs - fromMs), live: range.mode === "live" }
}

function pickStep(durationMs: number): number {
    return niceSteps.find(step => durationMs / step <= targetBuckets) ?? niceSteps.at(-1)!
}
