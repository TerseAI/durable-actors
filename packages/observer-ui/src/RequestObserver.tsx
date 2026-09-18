import { Fragment, useEffect, useState } from "react"

import { ChevronRight, Pause, Play, RefreshCw } from "lucide-react"

import type { ObserverClient, RequestTrace, RequestTracePage } from "./client.js"
import { Badge } from "./components/ui/badge.js"
import { Button } from "./components/ui/button.js"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "./components/ui/table.js"
import { useRequests } from "./observer-hooks.js"

interface RequestObserverProps {
    client: Pick<ObserverClient, "watchRequests">
}

function RequestObserver({ client }: RequestObserverProps) {
    const { page, failed, retry } = useRequests(client)
    const [frozen, setFrozen] = useState<RequestTracePage>()
    const [selected, setSelected] = useState<string>()
    const shown = frozen ?? page
    const records = shown?.records ?? []
    useEffect(() => setFrozen(undefined), [client])
    return (
        <section className="la-observer la-requests" aria-label="Request observer">
            <div className="la-observer-toolbar">
                <div>
                    <h1>Requests</h1>
                    <p>Method calls and WebSocket events, with time spent waiting and processing.</p>
                </div>
                <div className="la-request-actions">
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
                </div>
            </div>
            {failed && (
                <div className="la-observer-error" role="alert">
                    Request stream disconnected. Reconnecting… {page ? "Showing the last received requests." : "Check your connection and runtime version."}
                </div>
            )}
            {!!page?.dropped && (
                <div className="la-observer-error" role="alert">
                    {page.dropped.toLocaleString()} request traces were not delivered. This history is incomplete.
                </div>
            )}
            <div className="la-observer-table-frame">
                <Table aria-label="Recent requests" className="la-request-table">
                    <TableHeader>
                        <TableRow>
                            <TableHead scope="col">Time</TableHead>
                            <TableHead scope="col">Request</TableHead>
                            <TableHead scope="col">Transport</TableHead>
                            <TableHead scope="col">Outcome</TableHead>
                            <TableHead scope="col">Total</TableHead>
                            <TableHead scope="col">Queue wait</TableHead>
                        </TableRow>
                    </TableHeader>
                    <TableBody>
                        {records.map(record => (
                            <Fragment key={`${shown!.epoch}-${record.sequence}`}>
                                <TableRow data-state={selected === `${shown!.epoch}-${record.sequence}` ? "selected" : undefined}>
                                    <TableCell>
                                        <time dateTime={new Date(record.startedAtMs).toISOString()} title={new Date(record.startedAtMs).toLocaleString()}>
                                            {new Date(record.startedAtMs).toLocaleTimeString([], { hour12: false })}
                                        </time>
                                    </TableCell>
                                    <TableCell>
                                        <Button
                                            variant="ghost"
                                            className="la-request-operation"
                                            aria-label={`Inspect ${record.operation} request`}
                                            aria-expanded={selected === `${shown!.epoch}-${record.sequence}`}
                                            onClick={() => setSelected(current => (current === `${shown!.epoch}-${record.sequence}` ? undefined : `${shown!.epoch}-${record.sequence}`))}
                                        >
                                            <ChevronRight aria-hidden="true" className={selected === `${shown!.epoch}-${record.sequence}` ? "la-request-expanded" : undefined} />
                                            <span>
                                                {record.operation}
                                                <small>
                                                    {record.actorType} / {record.actorId}
                                                </small>
                                            </span>
                                        </Button>
                                    </TableCell>
                                    <TableCell>{record.kind === "method" ? "Method" : "WebSocket"}</TableCell>
                                    <TableCell>
                                        <Badge variant="outline" className={`la-request-outcome-${record.outcome}`}>
                                            {record.outcome[0]!.toUpperCase() + record.outcome.slice(1)}
                                        </Badge>
                                    </TableCell>
                                    <TableCell>{duration(record.durationMs)}</TableCell>
                                    <TableCell>{record.queueWaitMs === null ? <span title="Request did not begin processing">—</span> : duration(record.queueWaitMs)}</TableCell>
                                </TableRow>
                                {selected === `${shown!.epoch}-${record.sequence}` && (
                                    <TableRow>
                                        <TableCell colSpan={6}>
                                            <RequestDetails record={record} />
                                        </TableCell>
                                    </TableRow>
                                )}
                            </Fragment>
                        ))}
                    </TableBody>
                </Table>
                {!records.length && (
                    <div className="la-request-empty" role="status">
                        <strong>{!shown ? (failed ? "Requests unavailable" : "Connecting to requests…") : "No requests yet"}</strong>
                        <p>Call an actor method or send a WebSocket message to see its timings.</p>
                    </div>
                )}
            </div>
            <div className="la-observer-footnote">
                <span className="la-observer-refresh-status">
                    <span className={`la-observer-dot ${failed ? "la-observer-dot-unknown" : "la-observer-dot-live"}`} />
                    {frozen ? "Display paused · collection continues" : failed ? "Reconnecting…" : page ? "Live updates" : "Connecting…"}
                </span>
                <span>Latest {shown?.capacity ?? 500} requests on this control plane · history resets on restart.</span>
            </div>
            {!!shown?.evicted && <p className="la-request-note">{shown.evicted.toLocaleString()} older records have left this history window.</p>}
            <p className="la-request-note">
                Total includes queue wait, actor processing, and persistence. Queue wait includes the WebSocket message queue. Timings exclude the caller’s network round trip.
            </p>
        </section>
    )
}

function RequestDetails({ record }: { record: RequestTrace }) {
    return (
        <dl className="la-request-details">
            <div>
                <dt>Request ID</dt>
                <dd>{record.requestId}</dd>
            </div>
            <div>
                <dt>Host</dt>
                <dd>{record.hostId}</dd>
            </div>
            {record.connectionId && (
                <div>
                    <dt>Connection</dt>
                    <dd>{record.connectionId}</dd>
                </div>
            )}
        </dl>
    )
}

function duration(ms: number): string {
    return ms >= 1000 ? `${(ms / 1000).toLocaleString(undefined, { maximumFractionDigits: 2 })} s` : `${ms.toLocaleString(undefined, { maximumFractionDigits: 1 })} ms`
}

export { RequestObserver }
export type { RequestObserverProps }
