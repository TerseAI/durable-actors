import { memo, useId, useMemo, useState } from "react"

import { TimeRangePicker } from "./TimeRangePicker.js"
import type { ObserverClient } from "./client.js"
import { Badge } from "./components/ui/badge.js"
import { Button } from "./components/ui/button.js"
import { Input } from "./components/ui/input.js"
import { NativeSelect, NativeSelectOption } from "./components/ui/native-select.js"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "./components/ui/table.js"
import { type ActorLog, type LogQuery, displayedLogLimit } from "./log-data.js"
import { useLogs } from "./log-history.js"
import { type TimeRange, defaultTimeRange, resolveRange } from "./time-range.js"

export interface LogObserverProps {
    client: Pick<ObserverClient, "listLogs" | "watchLogs">
    timeRange?: TimeRange
    onTimeRangeChange?: (range: TimeRange) => void
}

export function LogObserver({ client, timeRange, onTimeRangeChange }: LogObserverProps) {
    const [filters, setFilters] = useState<LogQuery>({})
    const [localRange, setLocalRange] = useState<TimeRange>(defaultTimeRange)
    const range = timeRange ?? localRange
    const query = useMemo(() => ({ ...filters, ...resolveRange(range, Date.now()) }), [filters, range])
    const logs = useLogs(client, query)
    return (
        <section className="la-observer la-logs" aria-label="Actor logs">
            <div className="la-observer-toolbar">
                <div>
                    <h1>Logs</h1>
                    <p>Actor output as it happens, including background work.</p>
                </div>
                <Badge>{client.watchLogs ? (logs.liveFailed ? "Reconnecting" : "Live") : "History"}</Badge>
            </div>
            <LogFilters range={range} onRangeChange={onTimeRangeChange ?? setLocalRange} onSearch={setFilters} />
            {!client.listLogs && !client.watchLogs && <p role="status">Logs are unavailable for this connection.</p>}
            {logs.historyFailed && (
                <div className="la-observer-error" role="alert">
                    Log history unavailable. Live logs continue to appear.{" "}
                    <Button variant="outline" onClick={logs.retryHistory}>
                        Retry history
                    </Button>
                </div>
            )}
            {logs.liveFailed && (
                <p className="la-observer-error" role="alert">
                    Live logs disconnected. Reconnecting… Received logs remain visible.
                </p>
            )}
            {logs.persistenceFailed && (
                <p className="la-observer-error" role="alert">
                    Some logs could not be saved. Live logs remain visible, but history may be incomplete.
                </p>
            )}
            {logs.dropped > 0 && (
                <p className="la-observer-error" role="alert">
                    {logs.dropped.toLocaleString()} logs were not delivered. This view may be incomplete.
                </p>
            )}
            {logs.loading && <p role="status">Loading log history…</p>}
            <Table aria-label="Actor log entries">
                <TableHeader>
                    <TableRow>
                        <TableHead>Time</TableHead>
                        <TableHead>Level</TableHead>
                        <TableHead>Actor</TableHead>
                        <TableHead>Message</TableHead>
                    </TableRow>
                </TableHeader>
                <TableBody>
                    {logs.records.map(record => (
                        <LogRow key={record.eventId} record={record} />
                    ))}
                </TableBody>
            </Table>
            {!logs.records.length && !logs.loading && <p role="status">No matching logs yet.</p>}
            <div className="la-log-footer">
                <span>{logs.records.length.toLocaleString()} logs shown</span>
                {logs.records.length >= displayedLogLimit ? (
                    <span>Showing the newest {displayedLogLimit} logs. Narrow the filters or time range to inspect older entries.</span>
                ) : (
                    logs.nextCursor && (
                        <Button variant="outline" disabled={logs.loading} onClick={logs.loadOlder}>
                            Load older
                        </Button>
                    )
                )}
            </div>
        </section>
    )
}

function LogFilters({ range, onRangeChange, onSearch }: { range: TimeRange; onRangeChange: (range: TimeRange) => void; onSearch: (query: LogQuery) => void }) {
    const id = useId()
    return (
        <form
            className="la-request-history-filters"
            onSubmit={event => {
                event.preventDefault()
                const data = new FormData(event.currentTarget)
                onSearch(Object.fromEntries(["actorName", "actorId", "requestId", "level", "search"].map(name => [name, String(data.get(name) ?? "").trim() || undefined])))
            }}
        >
            <div className="la-request-history-range">
                <span>Time range</span>
                <TimeRangePicker value={range} onChange={onRangeChange} align="start" />
            </div>
            {[
                ["actorName", "Actor class"],
                ["actorId", "Actor ID"],
                ["requestId", "Request ID"],
                ["search", "Search logs"]
            ].map(([name, label]) => (
                <div className="la-request-history-field" key={name}>
                    <label htmlFor={`${id}-${name}`}>{label}</label>
                    <Input id={`${id}-${name}`} name={name} maxLength={name === "search" ? 512 : 256} placeholder={name === "search" ? "Message contains…" : "All"} />
                </div>
            ))}
            <div className="la-request-history-field">
                <label htmlFor={`${id}-level`}>Level</label>
                <NativeSelect id={`${id}-level`} name="level" defaultValue="">
                    <NativeSelectOption value="">All levels</NativeSelectOption>
                    {["TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL"].map(level => (
                        <NativeSelectOption key={level} value={level}>
                            {level}
                        </NativeSelectOption>
                    ))}
                </NativeSelect>
            </div>
            <Button type="submit" variant="outline">
                Search
            </Button>
        </form>
    )
}

const LogRow = memo(function LogRow({ record }: { record: ActorLog }) {
    const [expanded, setExpanded] = useState(false)
    const date = new Date(record.timestampMs)
    return (
        <TableRow>
            <TableCell>
                <time dateTime={date.toISOString()} title={date.toLocaleString()}>
                    {date.toLocaleTimeString([], { hour12: false, hour: "2-digit", minute: "2-digit", second: "2-digit", fractionalSecondDigits: 3 })}
                </time>
            </TableCell>
            <TableCell>
                <Badge className={`la-log-level-${record.level.toLowerCase()}`}>{record.level}</Badge>
            </TableCell>
            <TableCell>
                <strong>{record.actorName}</strong>
                <span className="la-log-actor-id">{record.actorId}</span>
            </TableCell>
            <TableCell className="la-log-message">
                <details onToggle={event => setExpanded(event.currentTarget.open)}>
                    <summary>{record.message || "Empty log line"}</summary>
                    {expanded && (
                        <>
                            <dl className="la-request-details">
                                {Object.entries({
                                    Request: record.requestId ?? "Background / no request",
                                    Stream: record.stream,
                                    Host: record.hostId,
                                    Session: record.sessionId,
                                    Event: record.eventId
                                }).map(([label, value]) => (
                                    <div key={label}>
                                        <dt>{label}</dt>
                                        <dd>{value}</dd>
                                    </div>
                                ))}
                            </dl>
                            {Object.keys(record.fields).length > 0 && <pre aria-label="Structured log fields">{JSON.stringify(record.fields, null, 2)}</pre>}
                        </>
                    )}
                </details>
            </TableCell>
        </TableRow>
    )
})
