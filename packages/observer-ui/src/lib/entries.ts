import type { EntryFilter, EntryHistogram, EntryRecord, EntrySource, SourceCounts } from "../types/Entries.js"
import type { LevelCounts, LogLevel } from "../types/Logs.js"
import type { RequestOutcome } from "../types/Requests.js"
import type { ResidencyPeriod } from "../types/Residency.js"
import type { TimeBounds } from "../types/TimeRange.js"

import { logLevels } from "./logs.js"
import { type Query, allows, matches, withoutKeys } from "./query.js"

/** One row of the Logs view: a record with the time, level and source it sorts and filters by. */
export type Entry = {
    key: string
    source: EntrySource
    atMs: number
    level: LogLevel
    record: EntryRecord
}

export const entrySources: EntrySource[] = ["console", "rpc", "websocket", "system"]

const outcomeLevels: Record<RequestOutcome, LogLevel> = { completed: "info", rerouted: "debug", rejected: "warn", interrupted: "warn", failed: "error" }
const cleanCloseCodes = [1000, 1001]

export function toEntry(record: EntryRecord): Entry {
    switch (record.type) {
        case "request":
            return {
                key: `request:${record.item.eventId}`,
                source: record.item.kind === "websocket" ? "websocket" : "rpc",
                atMs: record.item.startedAtMs,
                level: outcomeLevels[record.item.outcome],
                record
            }
        case "log":
            return { key: `log:${record.item.logId}`, source: "console", atMs: record.item.timestampMs, level: record.item.level, record }
        case "connectionOpened":
            return { key: `opened:${record.item.connectionId}`, source: "websocket", atMs: record.item.openedAtMs, level: "info", record }
        case "connectionClosed": {
            const { connectionId, closedAtMs, closeCode } = record.item
            return { key: `closed:${connectionId}`, source: "websocket", atMs: closedAtMs, level: closeCode === null || cleanCloseCodes.includes(closeCode) ? "info" : "warn", record }
        }
        case "kill":
            return { key: `kill:${record.item.actorId}:${record.item.atMs}`, source: "system", atMs: record.item.atMs, level: "error", record }
        case "lifecycle":
            return { key: `${record.item.kind}:${record.item.actorId}:${record.item.atMs}`, source: "system", atMs: record.item.atMs, level: "info", record }
    }
}

export function isEntryRecord(event: { type: string }): event is EntryRecord {
    return ["request", "log", "connectionOpened", "connectionClosed", "kill", "lifecycle"].includes(event.type)
}

/** Whether the histogram counts the entry: the search without its source and level clauses, which the chart applies. */
export function countedBy(entry: Entry, filter: EntryFilter): boolean {
    return inScope(entry, filter) && matches(entry, countQuery(filter.query))
}

/** Whether the list shows the entry. */
export function shownBy(entry: Entry, filter: EntryFilter): boolean {
    return inScope(entry, filter) && matches(entry, filter.query)
}

export function countQuery(query: Query): Query {
    return withoutKeys(query, ["source", "level"])
}

/** The levels the search's source and level clauses keep from one bucket. */
export function shownLevels(counts: SourceCounts, query: Query): LevelCounts {
    const total: LevelCounts = { debug: 0, info: 0, warn: 0, error: 0 }
    for (const source of entrySources) if (allows(query, "source", source)) for (const level of logLevels) if (allows(query, "level", level)) total[level] += counts[source][level]
    return total
}

function inScope({ record }: Entry, { actorName, actorId }: EntryFilter): boolean {
    return record.item.actorName === actorName && (actorId === null || record.item.actorId === actorId)
}

export function emptySourceCounts(): SourceCounts {
    const empty = () => ({ debug: 0, info: 0, warn: 0, error: 0 })
    return { console: empty(), rpc: empty(), websocket: empty(), system: empty() }
}

export function emptyHistogram({ fromMs, toMs }: TimeBounds, bucketMs: number): EntryHistogram {
    return { fromMs, bucketMs, buckets: Array.from({ length: Math.max(1, Math.ceil((toMs - fromMs) / bucketMs)) }, emptySourceCounts) }
}

/** Adds each entry to the bucket it falls in. */
export function addCounts(histogram: EntryHistogram, entries: Entry[]): EntryHistogram {
    const buckets = histogram.buckets.map(counts => ({ ...counts }))
    for (const { atMs, source, level } of entries) {
        const index = Math.floor((atMs - histogram.fromMs) / histogram.bucketMs)
        if (buckets[index]) buckets[index] = { ...buckets[index], [source]: { ...buckets[index][source], [level]: buckets[index][source][level] + 1 } }
    }
    return { ...histogram, buckets }
}

/**
 * Wakes and sleeps read off residency periods: a live period that follows a dormant one, or that starts inside the
 * window with nothing before it, starts with a wake; a dormant period that follows a live one starts with a sleep.
 */
export function lifecycleRecords(actorName: string, periods: ResidencyPeriod[], windowFromMs: number): EntryRecord[] {
    const records: EntryRecord[] = []
    const previous = new Map<string, ResidencyPeriod>()
    for (const period of [...periods].sort((a, b) => a.fromMs - b.fromMs)) {
        const before = previous.get(period.actorId)
        previous.set(period.actorId, period)
        const { actorId, fromMs: atMs } = period
        if (period.status === "live" && atMs > windowFromMs && (before === undefined || before.status === "dormant"))
            records.push({ type: "lifecycle", item: { actorName, actorId, atMs, kind: "started" } })
        if (period.status === "dormant" && before?.status === "live")
            records.push({ type: "lifecycle", item: { actorName, actorId, atMs, kind: "stopped", liveForMs: before.fromMs > windowFromMs ? atMs - before.fromMs : undefined } })
    }
    return records
}

/** Newest first, each entry once. */
export function newestFirst(entries: Entry[]): Entry[] {
    const unique = new Map(entries.map(entry => [entry.key, entry]))
    return [...unique.values()].sort((a, b) => b.atMs - a.atMs || (a.key < b.key ? 1 : -1))
}
