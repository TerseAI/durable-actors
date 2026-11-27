import type { ApplyEvents } from "../../hooks/useLiveView.js"
import type { EntryFilter, EntryHistogram, SourceCounts } from "../../types/Entries.js"
import type { ObserveEvent } from "../../types/Events.js"
import type { TimeBounds } from "../../types/TimeRange.js"
import { type Entry, countedBy, emptySourceCounts, isEntryRecord, newestFirst, shownBy, toEntry } from "../entries.js"

const streamedLimit = 1_000

/** Histogram counts by bucket start, plus the time the store was read: later entries are counted here. */
export interface LiveEntryHistogram {
    bucketMs: number
    readAtMs: number
    counts: Record<number, SourceCounts>
    partial?: boolean
}

/** Prepends streamed entries the list shows. */
export function applyEntryEvents(filter: EntryFilter): ApplyEvents<Entry[]> {
    return (entries, events) => {
        const added = entriesIn(events).filter(entry => shownBy(entry, filter))
        return added.length === 0 ? entries : newestFirst([...added, ...entries]).slice(0, streamedLimit)
    }
}

export function liveEntryHistogram({ fromMs, bucketMs, buckets, partial }: EntryHistogram, readAtMs: number): LiveEntryHistogram {
    const counts: Record<number, SourceCounts> = {}
    buckets.forEach((bucket, index) => (counts[fromMs + index * bucketMs] = bucket))
    return { bucketMs, readAtMs, counts, partial }
}

/**
 * Counts each streamed entry once: the read covered what was stored before readAtMs. Hosts
 * export a request when it finishes, so one that finished after the read is counted here.
 */
export function applyEntryCounts(filter: EntryFilter): ApplyEvents<LiveEntryHistogram> {
    return (histogram, events) => {
        const added = entriesIn(events).filter(entry => countedBy(entry, filter) && finishedAtMs(entry) >= histogram.readAtMs)
        if (added.length === 0) return histogram
        const counts = { ...histogram.counts }
        for (const entry of added) {
            const start = Math.floor(entry.atMs / histogram.bucketMs) * histogram.bucketMs
            const bucket = counts[start] ?? emptySourceCounts()
            counts[start] = { ...bucket, [entry.source]: { ...bucket[entry.source], [entry.level]: bucket[entry.source][entry.level] + 1 } }
        }
        return { ...histogram, counts }
    }
}

/** The histogram drawn for aligned bounds, which move with a live window. */
export function entryHistogramFor({ bucketMs, counts, partial }: LiveEntryHistogram, { fromMs, toMs }: TimeBounds): EntryHistogram {
    const buckets: SourceCounts[] = []
    for (let start = fromMs; start < toMs; start += bucketMs) buckets.push(counts[start] ?? emptySourceCounts())
    return { fromMs, bucketMs, buckets, partial }
}

function entriesIn(events: ObserveEvent[]): Entry[] {
    return events.filter(isEntryRecord).map(toEntry)
}

function finishedAtMs(entry: Entry): number {
    return entry.record.type === "request" ? entry.atMs + entry.record.item.durationMs : entry.atMs
}
