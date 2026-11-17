import type { ApplyEvents } from "../../hooks/useLiveView.js"
import type { ObserveEvent } from "../../types/Events.js"
import type { ActorLog, LevelCounts, LogHistogram, LogLevel } from "../../types/Logs.js"
import type { TimeBounds } from "../../types/TimeRange.js"
import { logLevels } from "../logs.js"

const streamedLimit = 2_000

/** Histogram counts by bucket start, plus the time the store was read: later lines are counted here. */
export interface LiveHistogram {
    bucketMs: number
    readAtMs: number
    counts: Record<number, LevelCounts>
}

/** Prepends streamed lines at or above the level the list shows. */
export function applyLogEvents(level: LogLevel | null): ApplyEvents<ActorLog[]> {
    const shown = logLevels.slice(level ? logLevels.indexOf(level) : 0)
    return (lines, events) => {
        const added = logsIn(events).filter(line => shown.includes(line.level))
        if (added.length === 0) return lines
        const known = new Set(lines.map(line => line.logId))
        return [...added.filter(line => !known.has(line.logId)).reverse(), ...lines].slice(0, streamedLimit)
    }
}

export function liveHistogram({ fromMs, bucketMs, buckets }: LogHistogram, readAtMs: number): LiveHistogram {
    const counts: Record<number, LevelCounts> = {}
    buckets.forEach((bucket, index) => (counts[fromMs + index * bucketMs] = bucket))
    return { bucketMs, readAtMs, counts }
}

// Each line counts once: the read covered lines before readAtMs, the stream adds the rest.
export const applyHistogramEvents: ApplyEvents<LiveHistogram> = (histogram, events) => {
    const lines = logsIn(events).filter(line => line.timestampMs >= histogram.readAtMs && logLevels.includes(line.level))
    if (lines.length === 0) return histogram
    const counts = { ...histogram.counts }
    for (const line of lines) {
        const start = Math.floor(line.timestampMs / histogram.bucketMs) * histogram.bucketMs
        counts[start] = { ...(counts[start] ?? emptyCounts()), [line.level]: (counts[start]?.[line.level] ?? 0) + 1 }
    }
    return { ...histogram, counts }
}

/** The histogram drawn for aligned bounds, which move with a live window. */
export function histogramFor({ bucketMs, counts }: LiveHistogram, { fromMs, toMs }: TimeBounds): LogHistogram {
    const buckets: LevelCounts[] = []
    for (let start = fromMs; start < toMs; start += bucketMs) buckets.push(counts[start] ?? emptyCounts())
    return { fromMs, bucketMs, buckets }
}

function logsIn(events: ObserveEvent[]): ActorLog[] {
    return events.flatMap(event => (event.type === "log" ? [event.item] : []))
}

function emptyCounts(): LevelCounts {
    return { debug: 0, info: 0, warn: 0, error: 0 }
}
