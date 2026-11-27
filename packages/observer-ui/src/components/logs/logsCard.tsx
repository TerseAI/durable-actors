import { useData } from "../../client/DatatContext.js"
import { useDrawer } from "../../client/DrawerContext.js"
import { useSetTimeRange } from "../../client/TimeRangeContext.js"
import { useEntries } from "../../hooks/useEntries.js"
import { useLogQuery } from "../../hooks/useLogQuery.js"
import { type Entry, entrySources, shownLevels } from "../../lib/entries.js"
import { constructorRequestId, logLevels, shortRequestId } from "../../lib/logs.js"
import { type Query, type QueryKey, allows, fieldOf, withClause } from "../../lib/query.js"
import type { EntryHistogram, EntrySource } from "../../types/Entries.js"
import type { LevelCounts, LogHistogram, LogLevel } from "../../types/Logs.js"
import type { RequestOutcome, RequestTrace } from "../../types/Requests.js"
import type { TimeWindow } from "../../types/TimeRange.js"
import { RequestDetails } from "../instance/requestDetails.js"
import { ColumnsMenu, RecordTable, shownColumns, useHiddenColumns } from "../ui/recordTable.js"
import { SectionCard } from "../ui/sectionCard.js"

import { entryColumns } from "./entryColumns.js"
import { EntryDetails } from "./entryDetails.js"
import { LogChart } from "./logChart.js"
import { QuerySearch, type Suggestion } from "./querySearch.js"

const outcomes: RequestOutcome[] = ["completed", "failed", "rejected", "rerouted", "interrupted"]
const socketCallbacks = ["onConnect", "onMessage", "onDisconnect", "onAlarm"]
const recentIds = 20
const keyHints: [QueryKey, string][] = [
    ["source", "console, rpc, websocket, system"],
    ["level", "debug, info, warn, error"],
    ["instance", "an instance of this type"],
    ["request", "a request ID"],
    ["connection", "a connection ID"],
    ["operation", "a method or socket callback"],
    ["outcome", "completed, failed, rejected…"]
]

export type LogsCardProps = {
    actorName: string
    /** One instance's log; the whole class's when left out. */
    actorId?: string
    methods?: string[]
    timeWindow: TimeWindow
}

export function LogsCard({ actorName, actorId, methods, timeWindow }: LogsCardProps) {
    const { baseUrl } = useData()
    const setRange = useSetTimeRange()
    const drawer = useDrawer()
    const { text, setText, query } = useLogQuery()
    const logs = useEntries(baseUrl, timeWindow, { actorName, actorId: actorId ?? null, query })
    const columns = entryColumns({ search: query.terms[0] ?? "", showInstance: !actorId, onFilter: filterBy })
    const [hidden, setHidden] = useHiddenColumns(actorId ? "logs" : "class-logs", columns)
    function openDetails(entry: Entry, trigger: HTMLElement) {
        const { record } = entry
        const content =
            record.type === "request" ? (
                <RequestDetails call={record.item} blocker={blockerOf(record.item, logs.entries ?? [])} onShowLogs={() => filterBy("request", record.item.requestId)} />
            ) : (
                <EntryDetails entry={entry} record={record} onShowConnection={id => filterBy("connection", id)} />
            )
        drawer.show({ key: entry.key, content }, trigger)
    }
    function filterBy(key: QueryKey, value: string) {
        drawer.close()
        setText(withClause(text, key, value))
    }
    return (
        <SectionCard title="Logs" aside={<ColumnsMenu columns={columns} hidden={hidden} onChange={setHidden} />} busy={!logs.entries && !logs.isError} className="pb-3.5">
            {logs.isError ? (
                <p className="py-4 text-[13px] text-muted-foreground">Couldn't reach the observer API.</p>
            ) : (
                <>
                    <LogChart
                        histogram={logs.histogram && chartOf(logs.histogram, query)}
                        level={null}
                        live={timeWindow.live}
                        partial={logs.histogram?.partial}
                        onZoom={span => setRange({ mode: "static", ...span })}
                    />
                    <QuerySearch
                        value={text}
                        onChange={setText}
                        keys={actorId ? keyHints.filter(([key]) => key !== "instance") : keyHints}
                        valuesFor={key => suggestionsFor(key, query, logs.histogram, logs.entries ?? [], methods)}
                        placeholder="Search, or filter with source:, level:, request:…"
                    />
                    <RecordTable
                        columns={shownColumns(columns, hidden)}
                        rows={logs.entries}
                        rowKey={entry => entry.key}
                        noun="logs"
                        filtered={text.trim() !== ""}
                        onClearFilters={() => setText("")}
                        onReachEnd={logs.hasOlder && !logs.loadingOlder ? logs.loadOlder : undefined}
                        loadingOlder={logs.loadingOlder}
                        onOpen={openDetails}
                        selectedKey={drawer.openKey}
                    />
                </>
            )}
        </SectionCard>
    )
}

function suggestionsFor(key: QueryKey, query: Query, histogram: EntryHistogram | undefined, entries: Entry[], methods: string[] | undefined): Suggestion[] {
    switch (key) {
        case "source":
            return entrySources.map(source => ({ value: source, count: histogram && sourceTotal(histogram, query, source) }))
        case "level":
            return logLevels.map(level => ({ value: level, count: histogram && levelTotal(histogram, query, level) }))
        case "outcome":
            return outcomes.map(outcome => ({ value: outcome }))
        case "operation":
            return unique([...(methods ?? []), ...socketCallbacks, ...seen(entries, key)]).map(value => ({ value }))
        case "instance":
            return seen(entries, key).map(value => ({ value }))
        case "request":
        case "connection":
            return seen(entries, key)
                .slice(0, recentIds)
                .map(value => ({ value, label: shortRequestId(value) }))
    }
}

// The values the loaded entries have, newest first.
function seen(entries: Entry[], key: QueryKey): string[] {
    return unique(entries.map(entry => fieldOf(entry, key)).filter(value => value !== null && value !== constructorRequestId))
}

function unique(values: (string | null)[]): string[] {
    return [...new Set(values.filter(value => value !== null))]
}

function chartOf({ fromMs, bucketMs, buckets }: EntryHistogram, query: Query): LogHistogram {
    return { fromMs, bucketMs, buckets: buckets.map(counts => shownLevels(counts, query)) }
}

function sourceTotal({ buckets }: EntryHistogram, query: Query, source: EntrySource): number {
    return buckets.reduce((total, counts) => total + sumLevels(counts[source], level => allows(query, "level", level)), 0)
}

function levelTotal({ buckets }: EntryHistogram, query: Query, level: LogLevel): number {
    return buckets.reduce((total, counts) => total + entrySources.filter(source => allows(query, "source", source)).reduce((sum, source) => sum + counts[source][level], 0), 0)
}

function sumLevels(counts: LevelCounts, kept: (level: LogLevel) => boolean): number {
    return logLevels.filter(kept).reduce((total, level) => total + counts[level], 0)
}

// The request on the same instance this one queued behind, when both are on the list.
function blockerOf(call: RequestTrace, entries: Entry[]): RequestTrace | undefined {
    if (!call.queueWaitMs) return undefined
    for (const { record } of entries) {
        if (record.type !== "request") continue
        const other = record.item
        if (other.actorId !== call.actorId || other.eventId === call.eventId) continue
        if (other.startedAtMs + (other.queueWaitMs ?? 0) <= call.startedAtMs && call.startedAtMs < other.startedAtMs + other.durationMs) return other
    }
    return undefined
}
