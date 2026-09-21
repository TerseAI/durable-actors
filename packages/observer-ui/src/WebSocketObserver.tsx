import { useEffect, useMemo, useRef, useState } from "react"

import { RefreshCw, Search } from "lucide-react"

import { SocketTimeline, durationLabel, shortId, statusLabel } from "./SocketTimeline.js"
import type { ObserverClient } from "./client.js"
import { Button } from "./components/ui/button.js"
import { Input } from "./components/ui/input.js"
import { Sheet, SheetContent, SheetDescription, SheetTitle } from "./components/ui/sheet.js"
import { useInventory } from "./observer-hooks.js"
import { useSocketHistory } from "./socket-history.js"
import { formatDuration, sessionDuration, sessionSummary, socketSessions } from "./socket-sessions.js"
import type { SocketSession, SocketSessionStatus } from "./socket-sessions.js"

const windows = [
    { minutes: 15, label: "Last 15 minutes" },
    { minutes: 60, label: "Last hour" },
    { minutes: 24 * 60, label: "Last 24 hours" },
    { minutes: 7 * 24 * 60, label: "Last 7 days" },
    { minutes: 0, label: "All retained" }
]

interface WebSocketObserverProps {
    client: ObserverClient
    onSelectActor?: (actorName: string) => void
}

export function WebSocketObserver({ client, onSelectActor }: WebSocketObserverProps) {
    const { inventory, failed, retry } = useInventory(client)
    const [minutes, setMinutes] = useState(60)
    const [now, setNow] = useState(Date.now)
    const [query, setQuery] = useState("")
    const [status, setStatus] = useState<SocketSessionStatus | "all">("all")
    const [selected, setSelected] = useState<SocketSession>()
    const container = useRef<HTMLElement>(null)
    const trigger = useRef<HTMLElement | null>(null)
    useEffect(() => {
        const timer = setInterval(() => setNow(Date.now()), 5_000)
        return () => clearInterval(timer)
    }, [])
    const windowStart = minutes ? now - minutes * 60_000 : undefined
    const history = useSocketHistory(client, minutes ? Math.floor(windowStart! / 60_000) * 60_000 : undefined)
    const sessions = useMemo(() => socketSessions(history.rows ?? [], inventory), [history.rows, inventory])
    const ready = !!inventory || failed
    const needle = query.trim().toLocaleLowerCase()
    const visible = sessions
        .filter(
            session =>
                (status === "all" || session.status === status) &&
                (!needle || `${session.connectionId} ${session.actorName} ${session.actorId} ${session.hostId ?? ""}`.toLocaleLowerCase().includes(needle))
        )
        .sort((a, b) => Number(b.status === "open") - Number(a.status === "open") || (b.openedAtMs ?? b.lastSeenMs ?? Infinity) - (a.openedAtMs ?? a.lastSeenMs ?? Infinity))
    const summary = sessionSummary(sessions, now)
    const longest = Math.max(0, ...sessions.filter(session => session.status !== "open").map(session => sessionDuration(session, now)?.ms ?? 0)) || summary.p95
    const timelineStart = timelineOrigin(visible, windowStart, now)
    useEffect(() => setSelected(undefined), [client, minutes])
    return (
        <section ref={container} className="la-observer overview websockets" aria-label="WebSocket observer">
            <div className="overview-heading">
                <div>
                    <h1>WebSockets</h1>
                    <p>
                        {ready
                            ? `${summary.open.toLocaleString()} open now${history.supported ? ` · ${summary.total.toLocaleString()} ${summary.total === 1 ? "session" : "sessions"} ${windowLabel(minutes)}` : ""}`
                            : "Connecting to inventory…"}
                    </p>
                </div>
                <div className="overview-controls">
                    <span className="overview-updated">
                        {history.failed ? (
                            "History reconnecting…"
                        ) : history.updatedAt ? (
                            <>
                                Updated <time dateTime={new Date(history.updatedAt).toISOString()}>{new Date(history.updatedAt).toLocaleTimeString([], { hour12: false })}</time>
                            </>
                        ) : null}
                    </span>
                    {history.supported && (
                        <select aria-label="Time window" value={minutes} onChange={event => setMinutes(Number(event.target.value))}>
                            {windows.map(window => (
                                <option key={window.minutes} value={window.minutes}>
                                    {window.label}
                                </option>
                            ))}
                        </select>
                    )}
                    <Button
                        variant="outline"
                        size="icon"
                        aria-label="Refresh connections"
                        onClick={() => {
                            retry()
                            history.retry()
                        }}
                    >
                        <RefreshCw aria-hidden="true" />
                    </Button>
                </div>
            </div>
            {failed && (
                <p role="alert" className="overview-alert">
                    Could not refresh connections.{" "}
                    {inventory ? "Showing the last received inventory; it may be out of date." : "Open connections cannot be confirmed, so unfinished sessions show as lost."} Retrying automatically.
                </p>
            )}
            {history.failed && (
                <p role="alert" className="overview-alert">
                    Connection history unavailable. {history.rows ? "Showing the last loaded sessions." : "Only currently open connections are shown."} Retrying automatically.
                </p>
            )}
            <SummaryTiles summary={summary} ready={ready} history={history.supported && !!history.rows} minutes={minutes} />
            <div className="socket-filterbar">
                <div className="overview-search">
                    <Search aria-hidden="true" />
                    <Input aria-label="Filter connections" placeholder="Filter by connection, actor, instance, or host…" value={query} onInput={event => setQuery(event.currentTarget.value)} />
                </div>
                <select aria-label="Filter by status" value={status} onChange={event => setStatus(event.target.value as SocketSessionStatus | "all")}>
                    <option value="all">All statuses</option>
                    <option value="open">Open</option>
                    <option value="closed">Closed</option>
                    <option value="lost">Lost</option>
                </select>
            </div>
            {history.supported && (
                <section className="overview-panel socket-timeline-panel" aria-label="Connection timeline">
                    <div className="socket-panel-heading">
                        <h2>
                            Timeline <span>{visible.length.toLocaleString()}</span>
                        </h2>
                        <ul className="socket-legend" aria-label="Timeline legend">
                            <li>
                                <i className="socket-swatch socket-swatch-open" /> Open
                            </li>
                            <li>
                                <i className="socket-swatch socket-swatch-closed" /> Closed
                            </li>
                            <li>
                                <i className="socket-swatch socket-swatch-lost" /> Lost · no disconnect recorded
                            </li>
                        </ul>
                    </div>
                    {visible.length ? (
                        <SocketTimeline
                            sessions={visible}
                            start={timelineStart}
                            end={now}
                            selected={selected}
                            onSelect={(session, element) => {
                                trigger.current = element
                                setSelected(session)
                            }}
                        />
                    ) : (
                        <div className="overview-empty socket-timeline-empty" role="status">
                            <strong>{emptyTitle(ready, history, sessions.length)}</strong>
                            <p>{sessions.length ? "Try another filter or a wider time window." : "Sessions appear here as clients connect to actors."}</p>
                        </div>
                    )}
                </section>
            )}
            <div className="overview-panel">
                <div className="overview-table-scroll" role="region" aria-label="WebSocket connections" tabIndex={0}>
                    <table className="overview-table socket-table">
                        <thead>
                            <tr>
                                <th scope="col">Status</th>
                                <th scope="col">Connection</th>
                                <th scope="col">Actor class</th>
                                <th scope="col">Instance</th>
                                <th scope="col">Opened</th>
                                <th scope="col">Duration</th>
                                <th scope="col">Messages</th>
                                <th scope="col">Metadata</th>
                            </tr>
                        </thead>
                        <tbody>
                            {visible.map(session => (
                                <SessionRow
                                    key={JSON.stringify([session.actorName, session.actorId, session.connectionId])}
                                    session={session}
                                    now={now}
                                    longest={longest}
                                    selected={selected === session}
                                    onSelect={element => {
                                        trigger.current = element
                                        setSelected(session)
                                    }}
                                />
                            ))}
                        </tbody>
                    </table>
                </div>
                {!visible.length && (
                    <div className="overview-empty" role="status">
                        <strong>{emptyTitle(ready, history, sessions.length)}</strong>
                        <p>{sessions.length ? "Try another connection, actor, or status." : "Open an actor WebSocket to see its connection and metadata here."}</p>
                        {!!sessions.length && (
                            <Button
                                variant="outline"
                                onClick={() => {
                                    setQuery("")
                                    setStatus("all")
                                }}
                            >
                                Clear filter
                            </Button>
                        )}
                    </div>
                )}
            </div>
            <div className="overview-data-scope">
                <p>
                    {history.supported
                        ? `Sessions pair each connection’s onConnect and onDisconnect events from retained request history${history.capped ? "; only the 500 most recent sessions are shown" : ""}. Open connections follow the latest host report. Lost connections have no recorded disconnect and are no longer reported by a host, so their duration is a lower bound.`
                        : "Connections are active WebSockets, not unique people. Counts follow the latest host report."}
                </p>
            </div>
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
                    <SheetTitle>Connection details</SheetTitle>
                    <SheetDescription>Identifiers, timings, and metadata for this WebSocket.</SheetDescription>
                    {selected && <SessionDetails session={selected} now={now} onSelectActor={onSelectActor} />}
                </SheetContent>
            </Sheet>
        </section>
    )
}

function SummaryTiles({ summary, ready, history, minutes }: { summary: ReturnType<typeof sessionSummary>; ready: boolean; history: boolean; minutes: number }) {
    return (
        <div className="overview-metrics socket-metrics">
            <section className="overview-metric">
                <h2>Open now</h2>
                <div className="overview-value">
                    <strong aria-label="Open WebSocket connections">{ready ? summary.open.toLocaleString() : "—"}</strong>
                    <span>live connections</span>
                </div>
                <div className="overview-metric-foot">
                    <span>Latest host report</span>
                </div>
            </section>
            <section className="overview-metric">
                <h2>Sessions</h2>
                <div className="overview-value">
                    <strong aria-label="WebSocket sessions">{history ? summary.total.toLocaleString() : "—"}</strong>
                    <span>{windowLabel(minutes)}</span>
                </div>
                <div className="overview-metric-foot">
                    <span>
                        <b>{history ? (summary.total - summary.open - summary.lost).toLocaleString() : "—"}</b> closed
                    </span>
                    <span>
                        <b>{history ? summary.lost.toLocaleString() : "—"}</b> lost
                    </span>
                </div>
            </section>
            <section className="overview-metric">
                <h2>Session duration</h2>
                <div className="overview-value">
                    <strong aria-label="Median session duration">{history && summary.median !== null ? formatDuration(summary.median) : "—"}</strong>
                    <span>median</span>
                </div>
                <div className="overview-metric-foot">
                    <span>
                        <b>{history && summary.p95 !== null ? formatDuration(summary.p95) : "—"}</b> p95
                    </span>
                    <span>Open sessions count up to now</span>
                </div>
            </section>
            <section className="overview-metric">
                <h2>Messages</h2>
                <div className="overview-value">
                    <strong aria-label="WebSocket messages">{history ? summary.messages.toLocaleString() : "—"}</strong>
                    <span>received</span>
                </div>
                <div className="overview-metric-foot">
                    <span>
                        <b>{history && summary.messagesPerSession !== null ? summary.messagesPerSession.toLocaleString(undefined, { maximumFractionDigits: 1 }) : "—"}</b> per session
                    </span>
                </div>
            </section>
        </div>
    )
}

function SessionRow({ session, now, longest, selected, onSelect }: { session: SocketSession; now: number; longest: number | null; selected: boolean; onSelect: (element: HTMLElement) => void }) {
    const duration = sessionDuration(session, now)
    const share = duration && longest ? Math.min(1, duration.ms / longest) : 0
    return (
        <tr className="la-clickable-row" data-state={selected ? "selected" : undefined} onClick={event => onSelect(event.currentTarget.querySelector("button") ?? event.currentTarget)}>
            <td>
                <span className={`socket-status socket-status-${session.status}`}>
                    <i className={`socket-swatch socket-swatch-${session.status}`} aria-hidden="true" />
                    {statusLabel(session.status)}
                </span>
            </td>
            <td>
                <button type="button" className="socket-connection" title={session.connectionId} aria-label={`Inspect connection ${session.connectionId}`}>
                    {shortId(session.connectionId)}
                </button>
            </td>
            <td title={session.actorName}>{session.actorName}</td>
            <td title={session.actorId}>{session.actorId}</td>
            <td>
                {session.openedAtMs === null ? (
                    <span title="The connect event is not in retained history">—</span>
                ) : (
                    <time dateTime={new Date(session.openedAtMs).toISOString()} title={new Date(session.openedAtMs).toLocaleString()}>
                        {new Date(session.openedAtMs).toLocaleTimeString([], { hour12: false })}
                    </time>
                )}
            </td>
            <td>
                <span className="socket-duration">
                    <span>{durationLabel(session, now)}</span>
                    <i className={`socket-duration-bar socket-duration-${session.status}`} aria-hidden="true">
                        <b style={{ width: `${share * 100}%` }} />
                    </i>
                </span>
            </td>
            <td>{session.messages.toLocaleString()}</td>
            <td>
                {session.status === "open" ? (
                    <code className="socket-metadata">{JSON.stringify(session.metadata) ?? "undefined"}</code>
                ) : (
                    <span title="Metadata is only reported for open connections">—</span>
                )}
            </td>
        </tr>
    )
}

function SessionDetails({ session, now, onSelectActor }: { session: SocketSession; now: number; onSelectActor?: (actorName: string) => void }) {
    const fields: Record<string, string> = {
        Status: session.status === "lost" ? "Lost — no disconnect recorded" : statusLabel(session.status),
        Connection: session.connectionId,
        "Actor class": session.actorName,
        "Instance ID": session.actorId,
        Opened: session.openedAtMs === null ? "Before retained history" : new Date(session.openedAtMs).toLocaleString(),
        ...(session.closedAtMs !== null ? { Closed: new Date(session.closedAtMs).toLocaleString() } : {}),
        ...(session.status === "lost" && session.lastSeenMs !== null ? { "Last activity": new Date(session.lastSeenMs).toLocaleString() } : {}),
        Duration: durationLabel(session, now),
        Messages: session.messages.toLocaleString(),
        Failures: session.failures.toLocaleString(),
        Host: session.hostId ?? "Unknown"
    }
    return (
        <>
            <dl className="la-request-details">
                {Object.entries(fields).map(([label, value]) => (
                    <div key={label}>
                        <dt>{label}</dt>
                        <dd>{value}</dd>
                    </div>
                ))}
            </dl>
            {session.status === "open" && (
                <details className="socket-metadata-details" open>
                    <summary>Metadata</summary>
                    <pre>{JSON.stringify(session.metadata, null, 2) ?? "undefined"}</pre>
                </details>
            )}
            {onSelectActor && (
                <Button variant="outline" onClick={() => onSelectActor(session.actorName)}>
                    Open {session.actorName}
                </Button>
            )}
        </>
    )
}

// Zoom the axis to the sessions in view so short-lived connections stay visible in a wide window.
function timelineOrigin(sessions: SocketSession[], windowStart: number | undefined, now: number) {
    const earliest = Math.min(...sessions.map(session => session.openedAtMs ?? session.lastSeenMs ?? now))
    const origin = Number.isFinite(earliest) ? earliest - Math.max(5_000, (now - earliest) * 0.06) : now - 60_000
    return Math.max(windowStart ?? -Infinity, Math.min(origin, now - 10_000))
}

function windowLabel(minutes: number) {
    return minutes ? `in the ${windows.find(window => window.minutes === minutes)!.label.toLocaleLowerCase()}` : "in retained history"
}

function emptyTitle(ready: boolean, history: { supported: boolean; rows?: unknown; failed: boolean }, total: number) {
    if (!ready && !history.rows) return history.failed ? "Connections unavailable" : "Loading connections…"
    if (total) return "No matching connections"
    return history.supported ? "No WebSocket sessions" : "No active WebSockets"
}
