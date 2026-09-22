import { useEffect, useMemo, useRef, useState } from "react"

import { Pause, Play, RefreshCw } from "lucide-react"

import type { ObserverClient, RequestTrace, RequestTracePage } from "./client.js"
import type { RequestHistoryQuery } from "./client.js"
import { Badge } from "./components/ui/badge.js"
import { Button } from "./components/ui/button.js"
import { Input } from "./components/ui/input.js"
import { Sheet, SheetContent, SheetDescription, SheetTitle } from "./components/ui/sheet.js"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "./components/ui/table.js"
import { useRequests } from "./observer-hooks.js"
import { useRequestHistory } from "./request-history.js"

interface RequestObserverProps {
    client: Pick<ObserverClient, "watchRequests" | "listRequests">
    actor?: Pick<RequestTrace, "actorName" | "actorId">
}

function RequestObserver({ client, actor }: RequestObserverProps) {
    const [query, setQuery] = useState<RequestHistoryQuery>()
    const scopedQuery = useMemo(() => (query ? { ...query, ...actor } : undefined), [query, actor?.actorName, actor?.actorId])
    const history = useRequestHistory(client, scopedQuery)
    const { page, failed, retry } = useRequests(client)
    const [frozen, setFrozen] = useState<RequestTracePage>()
    const [selected, setSelected] = useState<RequestTrace>()
    const container = useRef<HTMLElement>(null)
    const trigger = useRef<HTMLButtonElement | null>(null)
    const Heading = actor ? "h3" : "h1"
    const shown = query ? history.page : (frozen ?? page)
    const statusPage = page ?? history.page
    const records = (shown?.records ?? []).filter(record => !actor || (record.actorName === actor.actorName && record.actorId === actor.actorId))
    useEffect(() => {
        setFrozen(undefined)
        setQuery(undefined)
    }, [client, actor?.actorName, actor?.actorId])
    useEffect(() => setSelected(undefined), [client, page?.epoch, query, actor?.actorName, actor?.actorId])
    return (
        <section ref={container} className="la-observer la-requests" aria-label="Request observer">
            <div className="la-observer-toolbar">
                <div>
                    <Heading>Requests</Heading>
                    <p>{actor ? "Method calls and WebSocket events for this instance." : "Method calls and WebSocket events, with time spent waiting and processing."}</p>
                </div>
                <div className="la-request-actions">
                    {client.listRequests && (
                        <>
                            <Button
                                variant={!query ? "secondary" : "outline"}
                                aria-pressed={!query}
                                onClick={() => {
                                    setQuery(undefined)
                                    setFrozen(undefined)
                                }}
                            >
                                Live
                            </Button>
                            <Button
                                variant={query ? "secondary" : "outline"}
                                aria-pressed={!!query}
                                onClick={() => {
                                    if (!query) setQuery({})
                                }}
                            >
                                History
                            </Button>
                        </>
                    )}
                    {!query && (
                        <>
                            <Button variant="outline" disabled={!shown} onClick={() => setFrozen(frozen ? undefined : page)}>
                                {frozen ? <Play aria-hidden="true" /> : <Pause aria-hidden="true" />}
                                {frozen ? "Resume" : "Pause"}
                            </Button>
                            {failed && (
                                <Button variant="outline" onClick={retry}>
                                    <RefreshCw aria-hidden="true" />
                                    Retry
                                </Button>
                            )}
                        </>
                    )}
                </div>
            </div>
            {query && <HistoryFilters query={query} loading={history.loading} onSearch={setQuery} scoped={!!actor} />}
            {query && history.failed && (
                <div className="la-observer-error" role="alert">
                    Request history unavailable.{" "}
                    <Button variant="outline" onClick={history.retry}>
                        Retry history
                    </Button>
                </div>
            )}
            {!query && failed && (
                <div className="la-observer-error" role="alert">
                    Request stream disconnected. Reconnecting… {page ? "Showing the last received requests." : "Check your connection and runtime version."}
                </div>
            )}
            {!!statusPage?.dropped && (
                <div className="la-observer-error" role="alert">
                    {statusPage.dropped.toLocaleString()} request traces were not delivered. This history is incomplete.
                </div>
            )}
            {statusPage?.persistenceFailed && (
                <div className="la-observer-error" role="alert">
                    Some request events could not be saved. This history may be incomplete.
                </div>
            )}
            {shown?.reset && (
                <div className="la-observer-error" role="alert">
                    Some older events are no longer retained. Showing available history.
                </div>
            )}
            <div className="la-observer-table-frame">
                <Table aria-label={query ? "Saved requests" : "Recent requests"} className="la-request-table">
                    <TableHeader>
                        <TableRow>
                            <TableHead scope="col">Time</TableHead>
                            <TableHead scope="col">Request</TableHead>
                            <TableHead scope="col">Instance</TableHead>
                            <TableHead scope="col">Transport</TableHead>
                            <TableHead scope="col">Outcome</TableHead>
                            <TableHead scope="col">Total</TableHead>
                            <TableHead scope="col">Queue wait</TableHead>
                            <TableHead scope="col">
                                <span className="la:sr-only">Details</span>
                            </TableHead>
                        </TableRow>
                    </TableHeader>
                    <TableBody>
                        {records.map(record => (
                            <TableRow
                                key={record.eventId ?? `${shown!.epoch}-${record.sequence}`}
                                className="la-clickable-row"
                                data-state={selected === record ? "selected" : undefined}
                                onClick={event => {
                                    trigger.current = event.currentTarget.querySelector("button")
                                    setSelected(record)
                                }}
                            >
                                <TableCell>
                                    <time dateTime={new Date(record.startedAtMs).toISOString()} title={new Date(record.startedAtMs).toLocaleString()}>
                                        {query ? new Date(record.startedAtMs).toLocaleString([], { hour12: false }) : new Date(record.startedAtMs).toLocaleTimeString([], { hour12: false })}
                                    </time>
                                </TableCell>
                                <TableCell>
                                    <span className="la-request-operation" title={record.operation}>
                                        {record.operation}
                                    </span>
                                </TableCell>
                                <TableCell>
                                    <span className="la-request-actor" title={`${record.actorName} / ${record.actorId}`}>
                                        {record.actorName} / {record.actorId}
                                    </span>
                                </TableCell>
                                <TableCell>{record.kind === "method" ? "Method" : "WebSocket"}</TableCell>
                                <TableCell>
                                    <Badge variant="outline" className={`la-request-outcome-${record.outcome}`}>
                                        {record.outcome[0]!.toUpperCase() + record.outcome.slice(1)}
                                    </Badge>
                                </TableCell>
                                <TableCell>{duration(record.durationMs)}</TableCell>
                                <TableCell>{record.queueWaitMs === null ? <span title="Request did not begin processing">—</span> : duration(record.queueWaitMs)}</TableCell>
                                <TableCell>
                                    <Button variant="ghost" size="sm" aria-label={`Inspect ${record.operation} request`} aria-haspopup="dialog">
                                        Details
                                    </Button>
                                </TableCell>
                            </TableRow>
                        ))}
                    </TableBody>
                </Table>
                {!records.length && (
                    <div className="la-request-empty" role="status">
                        <strong>
                            {query
                                ? history.loading
                                    ? "Loading history…"
                                    : history.failed
                                      ? "History unavailable"
                                      : "No saved requests in this range"
                                : !shown
                                  ? failed
                                      ? "Requests unavailable"
                                      : "Connecting to requests…"
                                  : "No requests yet"}
                        </strong>
                        <p>
                            {query
                                ? "Try a wider time range or fewer filters. Local history retains the latest 10,000 events."
                                : "Call an actor method or send a WebSocket message to see its timings."}
                        </p>
                    </div>
                )}
            </div>
            {query && shown?.nextCursor && (
                <Button className="la-request-load-older" variant="outline" disabled={history.loading} onClick={history.loadOlder}>
                    {history.loading ? "Loading…" : "Load older"}
                </Button>
            )}
            <div className="la-observer-footnote">
                <span className="la-observer-refresh-status">
                    {!query && <span className={`la-observer-dot ${failed ? "la-observer-dot-unknown" : "la-observer-dot-live"}`} />}
                    {query ? "Saved history" : frozen ? "Display paused · collection continues" : failed ? "Reconnecting…" : page ? "Live updates" : "Connecting…"}
                </span>
                <span>
                    {query
                        ? `${records.length.toLocaleString()} saved requests shown`
                        : actor
                          ? `${records.length} matching requests from the latest ${shown?.capacity ?? 500} on this control plane.`
                          : `Latest ${shown?.capacity ?? 500} requests on this control plane.`}
                </span>
            </div>
            {!query && !!shown?.evicted && <p className="la-request-note">{shown.evicted.toLocaleString()} older records have left this history window.</p>}
            <p className="la-request-note">
                Total includes queue wait, actor processing, and persistence. Queue wait includes the WebSocket message queue. Timings exclude the caller’s network round trip.
            </p>
            <Sheet
                open={!!selected}
                onOpenChange={open => {
                    if (!open) setSelected(undefined)
                }}
            >
                <SheetContent
                    container={container.current}
                    onCloseAutoFocus={event => {
                        event.preventDefault()
                        trigger.current?.focus()
                    }}
                >
                    <SheetTitle>Request details</SheetTitle>
                    <SheetDescription>Full identifiers and timings for this request.</SheetDescription>
                    {selected && <RequestDetails record={selected} />}
                </SheetContent>
            </Sheet>
        </section>
    )
}

function HistoryFilters({ query, loading, onSearch, scoped }: { query: RequestHistoryQuery; loading: boolean; onSearch: (query: RequestHistoryQuery) => void; scoped: boolean }) {
    const [invalid, setInvalid] = useState(false)
    return (
        <form
            className="la-request-history-filters"
            onSubmit={event => {
                event.preventDefault()
                const data = new FormData(event.currentTarget)
                const fromMs = data.get("from") ? new Date(String(data.get("from"))).getTime() : undefined
                const toMs = data.get("to") ? new Date(String(data.get("to"))).getTime() : undefined
                if (fromMs !== undefined && toMs !== undefined && fromMs > toMs) {
                    setInvalid(true)
                    return
                }
                setInvalid(false)
                onSearch({
                    fromMs,
                    toMs,
                    actorId: String(data.get("actorId") || "").trim() || undefined,
                    outcome: (String(data.get("outcome") || "") as RequestHistoryQuery["outcome"]) || undefined
                })
            }}
        >
            <label>
                From
                <Input type="datetime-local" name="from" />
            </label>
            <label>
                To
                <Input type="datetime-local" name="to" />
            </label>
            {!scoped && (
                <label>
                    Actor ID
                    <Input name="actorId" placeholder="All actors" maxLength={128} defaultValue={query.actorId} />
                </label>
            )}
            <label>
                Outcome
                <select className="la-observer-select" name="outcome" defaultValue={query.outcome ?? ""}>
                    <option value="">All outcomes</option>
                    {["completed", "failed", "rejected", "rerouted", "interrupted"].map(outcome => (
                        <option key={outcome} value={outcome}>
                            {outcome[0]!.toUpperCase() + outcome.slice(1)}
                        </option>
                    ))}
                </select>
            </label>
            <Button type="submit" variant="outline" disabled={loading}>
                Search
            </Button>
            {invalid && (
                <p className="la-observer-error" role="alert">
                    From must be before To.
                </p>
            )}
        </form>
    )
}

function RequestDetails({ record }: { record: RequestTrace }) {
    const fields = {
        Operation: record.operation,
        "Actor class": record.actorName,
        "Instance ID": record.actorId,
        "Request ID": record.requestId,
        Time: new Date(record.startedAtMs).toLocaleString(),
        Transport: record.kind === "method" ? "Method" : "WebSocket",
        Outcome: record.outcome,
        Total: duration(record.durationMs),
        "Queue wait": record.queueWaitMs === null ? "Did not begin processing" : duration(record.queueWaitMs),
        Host: record.hostId,
        ...(record.connectionId ? { Connection: record.connectionId } : {})
    }
    return (
        <dl className="la-request-details">
            {Object.entries(fields).map(([label, value]) => (
                <div key={label}>
                    <dt>{label}</dt>
                    <dd>{value}</dd>
                </div>
            ))}
        </dl>
    )
}

function duration(ms: number): string {
    return ms >= 1000 ? `${(ms / 1000).toLocaleString(undefined, { maximumFractionDigits: 2 })} s` : `${ms.toLocaleString(undefined, { maximumFractionDigits: 1 })} ms`
}

export { RequestObserver }
export type { RequestObserverProps }
