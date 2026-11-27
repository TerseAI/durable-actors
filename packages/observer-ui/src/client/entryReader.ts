import { type Entry, addCounts, countQuery, countedBy, emptyHistogram, toEntry } from "../lib/entries.js"
import { logLevels } from "../lib/logs.js"
import { type Query, type QueryKey, allows, isSimple, queryKeys, requiredValue, requires } from "../lib/query.js"
import type { EntryFilter, EntryHistogram, EntryPage, EntryRecord, EntrySource } from "../types/Entries.js"
import type { EventHistory } from "../types/Events.js"
import type { Histogram } from "../types/Histogram.js"
import type { ActorLog, ActorLogPage, LevelCounts, LogHistogram, LogLevel } from "../types/Logs.js"
import type { RequestCounts, RequestHistogram, RequestTrace, RequestTracePage } from "../types/Requests.js"
import type { SocketSession } from "../types/Sockets.js"
import type { TimeBounds } from "../types/TimeRange.js"

export type Get = <T>(path: string) => Promise<T>

type PagedSource = "console" | "request"
type SourceCursor = { next: string; oldestMs: number }
type Sources = Partial<Record<PagedSource, SourceCursor>>
// Later pages keep the first page's bounds: the server continues a source's pages only with the filters they began with.
type Cursor = { bounds: TimeBounds; sources: Sources }
type Read = { records: EntryRecord[]; cursor: SourceCursor | undefined }
// What a source adds to the chart: the server's counts, or records to count, which stop short when incomplete.
type Counted = { levels: [EntrySource, Histogram<LevelCounts>][]; records: EntryRecord[]; complete: boolean }
type Params = Record<string, string | null | undefined>

const pageSize = 200
// Reads one page can take while other sources keep its new records under the cutoff.
const maxReadsPerPage = 8
const countedPages = 4
const countedPageSize = 500

type Endpoint = "console" | "requests" | "connections" | "system"

// The sources each endpoint's records belong to and the fields they have; a query that excludes those
// sources, or requires another field, never matches them.
const endpoints: Record<Endpoint, { sources: EntrySource[]; fields: QueryKey[] }> = {
    console: { sources: ["console"], fields: ["source", "level", "instance", "request"] },
    requests: { sources: ["rpc", "websocket"], fields: ["source", "level", "instance", "request", "connection", "operation", "outcome"] },
    connections: { sources: ["websocket"], fields: ["source", "level", "instance", "connection"] },
    system: { sources: ["system"], fields: ["source", "level", "instance"] }
}

/**
 * Builds the Logs view from each source's own endpoint. Console lines and requests page newest first, and
 * each page reads the source that is furthest behind, so records above the cutoff have no gaps. Connections
 * and kills come whole with the first page. Lifecycle entries come from the residency view instead. Server
 * parameters only narrow what is read; the view applies the whole search to every record.
 */
export class EntryReader {
    constructor(private readonly get: Get) {}

    async listEntries(filter: EntryFilter, bounds: TimeBounds, cursor: string | null): Promise<EntryPage> {
        if (cursor !== null) {
            const { bounds: first, sources } = JSON.parse(cursor) as Cursor
            return this.fill(filter, first, [], sources)
        }
        const none: Read = { records: [], cursor: undefined }
        const [console, requests, whole] = await Promise.all([
            canMatch("console", filter.query) ? this.readConsole(filter, bounds, null) : none,
            canMatch("requests", filter.query) ? this.readRequests(filter, bounds, null) : none,
            this.readWhole(filter, bounds)
        ])
        return this.fill(filter, bounds, [...console.records, ...requests.records, ...whole], { console: console.cursor, request: requests.cursor })
    }

    // Counts every source and level so the chart and suggestions can apply the search's source and level clauses.
    async getEntryHistogram(filter: EntryFilter, bounds: TimeBounds, bucketMs: number): Promise<EntryHistogram> {
        const counting = { ...filter, query: countQuery(filter.query) }
        const none: Counted = { levels: [], records: [], complete: true }
        const [whole, console, requests] = await Promise.all([
            this.readWhole(counting, bounds),
            canMatch("console", counting.query) ? this.consoleCounts(counting, bounds, bucketMs) : none,
            canMatch("requests", counting.query) ? this.requestCounts(counting, bounds, bucketMs) : none
        ])
        const histogram = emptyHistogram(bounds, bucketMs)
        for (const [source, counts] of [...console.levels, ...requests.levels]) placeLevels(histogram, source, counts)
        const counted = [...whole, ...console.records, ...requests.records]
        return {
            ...addCounts(
                histogram,
                counted.map(toEntry).filter((entry: Entry) => countedBy(entry, filter))
            ),
            partial: !console.complete || !requests.complete
        }
    }

    private async fill(filter: EntryFilter, bounds: TimeBounds, records: EntryRecord[], cursor: Sources): Promise<EntryPage> {
        for (let reads = 0; reads < maxReadsPerPage && behind(cursor) && !records.some(record => toEntry(record).atMs >= cutoffOf(cursor)!); reads++) {
            const source = behind(cursor)!
            const next = cursor[source]!.next
            const read = source === "console" ? await this.readConsole(filter, bounds, next) : await this.readRequests(filter, bounds, next)
            records.push(...read.records)
            cursor = { ...cursor, [source]: read.cursor }
        }
        const cutoffMs = cutoffOf(cursor)
        return { records, nextCursor: cutoffMs === null ? null : JSON.stringify({ bounds, sources: cursor } satisfies Cursor), cutoffMs }
    }

    private async readConsole(filter: EntryFilter, bounds: TimeBounds, cursor: string | null, limit = pageSize): Promise<Read> {
        const { query } = filter
        const params = { requestId: requiredValue(query, "request"), level: lowestLevel(query), search: longestTerm(query), limit: String(limit), cursor }
        const page = await this.get<ActorLogPage>(`logs?${rangeParams(bounds, filter, params)}`)
        return paged(
            page.records.map((item: ActorLog) => ({ type: "log" as const, item })),
            page.nextCursor,
            item => item.timestampMs
        )
    }

    private async readRequests(filter: EntryFilter, bounds: TimeBounds, cursor: string | null, limit = pageSize): Promise<Read> {
        const { query } = filter
        const outcome = requiredValue(query, "outcome") ?? (requiredValue(query, "level") === "error" ? "failed" : null)
        const params = { requestId: requiredValue(query, "request"), connectionId: requiredValue(query, "connection"), outcome, limit: String(limit), cursor }
        const page = await this.get<RequestTracePage>(`requests?${rangeParams(bounds, filter, params)}`)
        return paged(
            page.records.map((item: RequestTrace) => ({ type: "request" as const, item })),
            page.nextCursor,
            item => item.startedAtMs
        )
    }

    private async readWhole(filter: EntryFilter, bounds: TimeBounds): Promise<EntryRecord[]> {
        const actorId = actorIdOf(filter)
        const [sessions, kills] = await Promise.all([
            canMatch("connections", filter.query) ? this.get<SocketSession[]>(`websockets?${rangeParams(bounds)}`) : [],
            canMatch("system", filter.query) ? this.get<EventHistory>(`events/history?${rangeParams(bounds, filter, { types: "kill" })}`).then(history => history.events) : []
        ])
        const ownSessions = sessions.filter(session => session.actorName === filter.actorName && (actorId === null || session.actorId === actorId))
        return [...ownSessions.flatMap(session => connectionRecords(session, bounds)), ...kills]
    }

    // The server counts what its parameters can say; anything finer counts the newest records themselves.
    private async consoleCounts(filter: EntryFilter, bounds: TimeBounds, bucketMs: number): Promise<Counted> {
        const { query } = filter
        if (isSimple(query, "request") && isSimple(query, "instance") && query.terms.length <= 1 && !query.excludedTerms.length) {
            const counts = await this.get<LogHistogram>(
                `logs/histogram?${rangeParams(bounds, filter, { requestId: requiredValue(query, "request"), search: longestTerm(query), bucketMs: String(bucketMs) })}`
            )
            return { levels: [["console", counts]], records: [], complete: true }
        }
        return await this.collect(cursor => this.readConsole(filter, bounds, cursor, countedPageSize))
    }

    private async requestCounts(filter: EntryFilter, bounds: TimeBounds, bucketMs: number): Promise<Counted> {
        const { query } = filter
        const narrowed = query.clauses.some(clause => clause.key !== "instance") || query.terms.length > 0 || query.excludedTerms.length > 0
        if (narrowed || !isSimple(query, "instance")) return await this.collect(cursor => this.readRequests(filter, bounds, cursor, countedPageSize))
        const read = (kind: RequestTrace["kind"]) => this.get<RequestHistogram>(`requests/histogram?${rangeParams(bounds, filter, { kind, bucketMs: String(bucketMs) })}`)
        const [methods, sockets] = await Promise.all([read("method"), read("websocket")])
        return {
            levels: [
                ["rpc", requestLevels(methods)],
                ["websocket", requestLevels(sockets)]
            ],
            records: [],
            complete: true
        }
    }

    private async collect(read: (cursor: string | null) => Promise<Read>): Promise<Counted> {
        const records: EntryRecord[] = []
        let cursor: string | null = null
        for (let page = 0; page < countedPages; page++) {
            const result: Read = await read(cursor)
            records.push(...result.records)
            if (!result.cursor) return { levels: [], records, complete: true }
            cursor = result.cursor.next
        }
        return { levels: [], records, complete: false }
    }
}

/** Whether any record the endpoint returns can match the search. */
function canMatch(endpoint: Endpoint, query: Query): boolean {
    const { sources, fields } = endpoints[endpoint]
    return sources.some(source => allows(query, "source", source)) && queryKeys.every(key => fields.includes(key) || !requires(query, key))
}

function actorIdOf({ actorId, query }: EntryFilter): string | null {
    return actorId ?? requiredValue(query, "instance")
}

// Every match is at this level or above, so the server can skip lower levels.
function lowestLevel({ clauses }: Query): LogLevel | null {
    const clause = clauses.find(candidate => candidate.key === "level" && !candidate.negated)
    const levels = logLevels.filter(level => clause?.values.includes(level))
    return levels[0] ?? null
}

// Every match contains each term, so the server can search for one of them.
function longestTerm({ terms }: Query): string | null {
    return [...terms].sort((a, b) => b.length - a.length)[0] ?? null
}

function rangeParams({ fromMs, toMs }: TimeBounds, filter?: EntryFilter, extra: Params = {}): URLSearchParams {
    const query = new URLSearchParams({ fromMs: String(fromMs), toMs: String(toMs) })
    if (filter) query.set("actorName", filter.actorName)
    const actorId = filter && actorIdOf(filter)
    if (actorId) query.set("actorId", actorId)
    for (const [key, value] of Object.entries(extra)) if (value) query.set(key, value)
    return query
}

function paged<T>(records: { type: EntryRecord["type"]; item: T }[], next: string | null, atMs: (item: T) => number): Read {
    const oldestMs = Math.min(...records.map(record => atMs(record.item)))
    return { records: records as EntryRecord[], cursor: next && records.length ? { next, oldestMs } : undefined }
}

/** The source whose oldest loaded record is newest: the next page reads it. */
function behind(cursor: Sources): PagedSource | undefined {
    const sources = (Object.keys(cursor) as PagedSource[]).filter(source => cursor[source])
    return sources.sort((a, b) => cursor[b]!.oldestMs - cursor[a]!.oldestMs)[0]
}

function cutoffOf(cursor: Sources): number | null {
    const source = behind(cursor)
    return source ? cursor[source]!.oldestMs : null
}

function connectionRecords(session: SocketSession, { fromMs, toMs }: TimeBounds): EntryRecord[] {
    const { connectionId, actorName, actorId, gatewayId, openedAtMs, closedAtMs, metadata } = session
    const records: EntryRecord[] = []
    if (openedAtMs >= fromMs && openedAtMs <= toMs) records.push({ type: "connectionOpened", item: { connectionId, actorName, actorId, gatewayId, openedAtMs, metadata } })
    if (session.status !== "open" && closedAtMs !== null && closedAtMs >= fromMs && closedAtMs <= toMs)
        records.push({
            type: "connectionClosed",
            item: {
                connectionId,
                actorName,
                actorId,
                gatewayId,
                openedAtMs,
                closedAtMs,
                closeCode: session.closeCode ?? null,
                closeReason: session.closeReason ?? (session.status === "lost" ? "connection lost" : null),
                messagesIn: session.messagesIn ?? null,
                messagesOut: session.messagesOut ?? null
            }
        })
    return records
}

function placeLevels(histogram: EntryHistogram, source: EntrySource, counts: Histogram<LevelCounts>): void {
    counts.buckets.forEach((levels, index) => {
        const bucket = histogram.buckets[Math.floor((counts.fromMs + index * counts.bucketMs - histogram.fromMs) / histogram.bucketMs)]
        if (bucket) for (const level of Object.keys(levels) as (keyof LevelCounts)[]) bucket[source][level] += levels[level]
    })
}

function requestLevels(histogram: RequestHistogram): Histogram<LevelCounts> {
    return { ...histogram, buckets: histogram.buckets.map(outcomeLevels) }
}

function outcomeLevels({ ok, failed, rejected, interrupted }: RequestCounts): LevelCounts {
    return { debug: 0, info: ok, warn: rejected + interrupted, error: failed }
}
