import { useEffect, useId, useRef, useState } from "react"

import { Box, CircleHelp, RefreshCw, Search, Unplug } from "lucide-react"

import { RequestObserver } from "./RequestObserver.js"
import type { ActorInstance, ActorInventory, ObserverClient } from "./client.js"
import { Badge } from "./components/ui/badge.js"
import { Button } from "./components/ui/button.js"
import { Input } from "./components/ui/input.js"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "./components/ui/table.js"
import { useInventory } from "./observer-hooks.js"
import { useQueueWaits } from "./queue-wait-history.js"
import { formatWait, queueWaitByActor, queueWaitByInstance, queueWaitTotal } from "./queue-wait.js"
import type { QueueWaitStats } from "./queue-wait.js"

interface ActorObserverProps {
    client: ObserverClient
    className?: string
    initialActorName?: string
    navigation?: { actorName?: string; onSelectActor: (actorName?: string) => void }
}

function ActorObserver({ client, className = "", initialActorName, navigation }: ActorObserverProps) {
    const { inventory, loading, failed, retry } = useInventory(client)
    const [localActorName, setLocalActorName] = useState(initialActorName)
    const selectedActorName = navigation ? navigation.actorName : localActorName
    const setSelectedActorName = navigation ? navigation.onSelectActor : setLocalActorName
    const [query, setQuery] = useState("")
    const heading = useRef<HTMLHeadingElement>(null)
    const previousActor = useRef(initialActorName)
    const detailsId = useId()
    const selectedActor = inventory?.actors.find(actor => actor.actorName === selectedActorName)
    const waits = useQueueWaits(client, selectedActorName)
    const waitsByActor = waits.rows && queueWaitByActor(waits.rows)
    const waitsByInstance = waits.rows && selectedActorName !== undefined ? queueWaitByInstance(waits.rows, selectedActorName) : undefined
    const totalWait = waits.supported ? (waits.rows ? queueWaitTotal(waits.rows) : undefined) : undefined
    useEffect(() => {
        setLocalActorName(initialActorName)
        setQuery("")
    }, [client, initialActorName])
    useEffect(() => {
        if (previousActor.current !== selectedActorName) heading.current?.focus()
        previousActor.current = selectedActorName
    }, [selectedActorName])
    return (
        <section className={`la-observer ${className}`} aria-label="Actor observer">
            {selectedActorName !== undefined && (
                <nav className="la-observer-breadcrumb" aria-label="Breadcrumb">
                    <Button variant="link" type="button" aria-label="Back to actors" onClick={() => setSelectedActorName(undefined)}>
                        Actors
                    </Button>
                    <span aria-hidden="true">/</span>
                    <span aria-current="page">{selectedActorName}</span>
                </nav>
            )}
            <div className="la-observer-toolbar">
                <div>
                    <div className="la-observer-title">
                        <h1 ref={heading} tabIndex={-1}>
                            {selectedActorName ?? "Actors"}
                        </h1>
                    </div>
                    <p>{selectedActorName === undefined ? "Inspect instances, residency, and connections." : "Inspect this actor class’s instances, residency, and connections."}</p>
                </div>
                <Button variant="outline" type="button" disabled={loading} onClick={retry}>
                    <RefreshCw aria-hidden="true" className={loading ? "la-observer-spin" : undefined} />
                    {loading ? "Refreshing…" : failed ? "Try again" : "Refresh"}
                </Button>
            </div>
            {failed && (
                <div className="la-observer-error" role="alert">
                    <CircleHelp aria-hidden="true" />
                    <p>{inventory ? "Could not refresh. These counts may be out of date." : "Could not load actors. Check your connection and access, then try again."}</p>
                </div>
            )}
            {!inventory && !failed && <InventorySkeleton />}
            {inventory && (
                <>
                    {selectedActorName !== undefined ? (
                        selectedActor ? (
                            <>
                                <InventorySummary inventory={{ actors: [selectedActor] }} actorName={selectedActorName} queueWait={waits.supported ? (totalWait ?? null) : undefined} />
                                <ActorInstances
                                    client={client}
                                    key={selectedActorName}
                                    id={detailsId}
                                    actorName={selectedActorName}
                                    instances={selectedActor.instances}
                                    waits={waits.supported ? waitsByInstance : undefined}
                                />
                            </>
                        ) : (
                            <EmptyState title="Actor class unavailable" description="This actor class is no longer in the latest inventory. Return to Actors to see available classes." />
                        )
                    ) : (
                        <>
                            <InventorySummary inventory={inventory} queueWait={waits.supported ? (totalWait ?? null) : undefined} />
                            {inventory.actors.length ? (
                                <ActorTable inventory={inventory} query={query} onQueryChange={setQuery} onSelectActor={setSelectedActorName} waits={waits.supported ? waitsByActor : undefined} />
                            ) : (
                                <EmptyState title="No actors yet" description="Deploy your actor classes to see them here. Instance counts appear as actors are used." />
                            )}
                        </>
                    )}
                    <div className="la-observer-footnote">
                        <span className="la-observer-refresh-status">
                            <span className={`la-observer-dot ${failed ? "la-observer-dot-unknown" : client.watchActors ? "la-observer-dot-live" : ""}`} />
                            {failed ? "Reconnecting…" : client.watchActors ? "Live updates" : "Auto-refresh every 5s"}
                        </span>
                        <span>{client.watchActors ? "Changes stream from the control plane." : "Counts follow the latest host heartbeat."}</span>
                    </div>
                </>
            )}
        </section>
    )
}

function InventorySkeleton() {
    return (
        <div className="la-observer-skeleton" role="status" aria-label="Loading actors">
            <span className="la:sr-only">Loading actors…</span>
            <div />
            <div />
            <div />
        </div>
    )
}

function InventorySummary({ inventory, actorName, queueWait }: { inventory: ActorInventory; actorName?: string; queueWait?: QueueWaitStats | null }) {
    const totals = inventory.actors.reduce((sum, actor) => ({ live: sum.live + actor.live, dormant: sum.dormant + actor.dormant, unknown: sum.unknown + actor.unknown }), {
        live: 0,
        dormant: 0,
        unknown: 0
    })
    return (
        <dl className="la-observer-summary">
            <div>
                <dt>Total instances</dt>
                <dd aria-label="Total instances">{(totals.live + totals.dormant + totals.unknown).toLocaleString()}</dd>
                <p>{actorName === undefined ? `${inventory.actors.length} actor ${inventory.actors.length === 1 ? "type" : "types"}` : "In this actor class"}</p>
            </div>
            <div>
                <dt>
                    <span className="la-observer-dot la-observer-dot-live" />
                    Live
                </dt>
                <dd aria-label="Live instances">{totals.live.toLocaleString()}</dd>
                <p>Loaded in memory</p>
            </div>
            <div>
                <dt>
                    <span className="la-observer-dot" />
                    Dormant
                </dt>
                <dd aria-label="Dormant instances">{totals.dormant.toLocaleString()}</dd>
                <p>Currently unloaded</p>
            </div>
            {totals.unknown > 0 && (
                <div>
                    <dt>
                        <span className="la-observer-dot la-observer-dot-unknown" />
                        Unknown
                    </dt>
                    <dd aria-label="Unknown instances">{totals.unknown.toLocaleString()}</dd>
                    <p>Awaiting host report</p>
                </div>
            )}
            {queueWait !== undefined && (
                <div className="la-observer-queue-wait">
                    <dt>Avg queue wait</dt>
                    <dd aria-label="Average queue wait">{formatWait(queueWait?.averageMs)}</dd>
                    <p>{queueWait ? `Last hour · max ${formatWait(queueWait.maxMs)} · ${queueWait.admitted.toLocaleString()} admitted` : "No admitted requests in the last hour"}</p>
                </div>
            )}
        </dl>
    )
}

function ActorTable({
    inventory,
    query,
    onQueryChange,
    onSelectActor,
    waits
}: {
    inventory: ActorInventory
    query: string
    onQueryChange: (query: string) => void
    onSelectActor: (actorName: string) => void
    waits?: Map<string, QueueWaitStats>
}) {
    const hasUnknown = inventory.actors.some(actor => actor.unknown > 0)
    const actors = inventory.actors.filter(actor => matches(actor.actorName, query)).sort((a, b) => Number(b.live > 0) - Number(a.live > 0))
    return (
        <>
            <div className="la-observer-list-toolbar">
                <h2>
                    Actor names <Badge>{inventory.actors.length}</Badge>
                </h2>
                <SearchField label="Search actors" placeholder="Search actors…" value={query} onChange={onQueryChange} />
            </div>
            <div className="la-observer-table-frame">
                {actors.length ? (
                    <Table aria-label="Actor instance counts" className="la-observer-table">
                        <TableHeader>
                            <TableRow>
                                <TableHead scope="col">Actor</TableHead>
                                <TableHead scope="col">Live</TableHead>
                                <TableHead scope="col">Dormant</TableHead>
                                {hasUnknown && <TableHead scope="col">Unknown</TableHead>}
                                <TableHead scope="col">Total</TableHead>
                                {waits && (
                                    <TableHead scope="col" className="la-observer-wait-cell">
                                        Avg queue wait
                                    </TableHead>
                                )}
                            </TableRow>
                        </TableHeader>
                        <TableBody>
                            {actors.map(actor => (
                                <TableRow key={actor.actorName} className="la-clickable-row" onClick={() => onSelectActor(actor.actorName)}>
                                    <TableCell className="la-observer-name">
                                        <Button variant="ghost" type="button" className="la-observer-actor-button la-observer-class-button">
                                            <Box className="la-observer-type-icon" aria-hidden="true" />
                                            <span>{actor.actorName}</span>
                                        </Button>
                                    </TableCell>
                                    <TableCell>
                                        <span className={actor.live > 0 ? "la-observer-live" : undefined}>{actor.live.toLocaleString()}</span>
                                    </TableCell>
                                    <TableCell>{actor.dormant.toLocaleString()}</TableCell>
                                    {hasUnknown && <TableCell>{actor.unknown.toLocaleString()}</TableCell>}
                                    <TableCell>{(actor.live + actor.dormant + actor.unknown).toLocaleString()}</TableCell>
                                    {waits && (
                                        <TableCell className="la-observer-wait-cell">
                                            <QueueWait stats={waits.get(actor.actorName)} />
                                        </TableCell>
                                    )}
                                </TableRow>
                            ))}
                        </TableBody>
                    </Table>
                ) : (
                    <EmptyState title="No matching actors" description="Try a different actor name." action="Clear search" onReset={() => onQueryChange("")} />
                )}
            </div>
            {hasUnknown && (
                <p className="la-observer-unknown">
                    <CircleHelp aria-hidden="true" />
                    Some hosts have not reported which instances are loaded. Their counts are shown as unknown.
                </p>
            )}
        </>
    )
}

function ActorInstances({ client, id, actorName, instances, waits }: { client: ObserverClient; id: string; actorName: string; instances: ActorInstance[]; waits?: Map<string, QueueWaitStats> }) {
    const [query, setQuery] = useState("")
    const [status, setStatus] = useState("all")
    const [selectedInstanceId, setSelectedInstanceId] = useState<string>()
    const connectionDetailsId = `${id}-connections`
    const filtered = instances.filter(instance => matches(instance.actorId, query) && (status === "all" || instance.status === status))
    filtered.sort((a, b) => Number(b.status === "live") - Number(a.status === "live"))
    const selectedInstance = instances.find(instance => instance.actorId === selectedInstanceId)
    const instanceHeading = useRef<HTMLHeadingElement>(null)
    useEffect(() => {
        instanceHeading.current?.focus()
    }, [selectedInstanceId])
    if (selectedInstanceId !== undefined)
        return (
            <section className="la-observer-instance-detail" aria-label={`${actorName} / ${selectedInstanceId}`}>
                <Button variant="link" aria-label="Back to instances" onClick={() => setSelectedInstanceId(undefined)}>
                    Back to instances
                </Button>
                <div className="la-observer-instance-heading">
                    <h2 ref={instanceHeading} tabIndex={-1}>
                        {selectedInstanceId}
                    </h2>
                    {selectedInstance && (
                        <Badge variant="outline" className={`la-observer-status-${selectedInstance.status}`}>
                            {labelStatus(selectedInstance.status)}
                        </Badge>
                    )}
                </div>
                {!selectedInstance && <p role="status">This instance is no longer in the current inventory. Its retained requests are still available below.</p>}
                {waits && <InstanceQueueWait stats={waits.get(selectedInstanceId)} />}
                {selectedInstance && <WaitingRequests waiting={selectedInstance.waiting} />}
                {client.watchRequests || client.query ? (
                    <RequestObserver key={selectedInstanceId} client={client} actor={{ actorName, actorId: selectedInstanceId }} />
                ) : (
                    <section>
                        <h3>Requests</h3>
                        <p>Request timings are unavailable for this connection.</p>
                    </section>
                )}
                {selectedInstance && <WebSocketConnections id={connectionDetailsId} actorId={selectedInstance.actorId} connections={selectedInstance.connections} />}
            </section>
        )
    return (
        <section id={id} className="la-observer-instances" aria-labelledby={`${id}-heading`}>
            <div className="la-observer-instances-heading">
                <div>
                    <h2 ref={instanceHeading} tabIndex={-1} id={`${id}-heading`}>
                        {actorName} instances <Badge aria-hidden="true">{instances.length.toLocaleString()}</Badge>
                    </h2>
                    <p>Select an instance to inspect its waiting line, requests, and WebSocket connections.</p>
                </div>
            </div>
            {instances.length ? (
                <>
                    <div className="la-observer-filters">
                        <SearchField label="Search instances" placeholder="Search instance IDs…" value={query} onChange={setQuery} />
                        <select className="la-observer-select" aria-label="Instance state" value={status} onChange={event => setStatus(event.target.value)}>
                            <option value="all">All states</option>
                            <option value="live">Live</option>
                            <option value="dormant">Dormant</option>
                            <option value="unknown">Unknown</option>
                        </select>
                    </div>
                    <div className="la-observer-table-frame">
                        {filtered.length ? (
                            <Table aria-label={`${actorName} instances`} className="la-observer-instance-table">
                                <TableHeader>
                                    <TableRow>
                                        <TableHead scope="col">Instance</TableHead>
                                        <TableHead scope="col">State</TableHead>
                                        <TableHead scope="col">Connections</TableHead>
                                        {waits && (
                                            <TableHead scope="col" className="la-observer-wait-cell">
                                                Avg queue wait
                                            </TableHead>
                                        )}
                                        <TableHead scope="col" className="la-observer-waiting-cell">
                                            Waiting
                                        </TableHead>
                                    </TableRow>
                                </TableHeader>
                                <TableBody>
                                    {filtered.map(instance => (
                                        <TableRow key={instance.actorId} className="la-clickable-row" onClick={() => setSelectedInstanceId(instance.actorId)}>
                                            <TableCell className="la-observer-instance-id">
                                                <Button variant="ghost" type="button" className="la-observer-actor-button">
                                                    <span>{instance.actorId}</span>
                                                </Button>
                                            </TableCell>
                                            <TableCell>
                                                <Badge variant="outline" className={`la-observer-status-${instance.status}`}>
                                                    <span className={`la-observer-dot la-observer-dot-${instance.status}`} />
                                                    {labelStatus(instance.status)}
                                                </Badge>
                                            </TableCell>
                                            <TableCell>{instance.connections.length.toLocaleString()}</TableCell>
                                            {waits && (
                                                <TableCell className="la-observer-wait-cell">
                                                    <QueueWait stats={waits.get(instance.actorId)} />
                                                </TableCell>
                                            )}
                                            <TableCell className="la-observer-waiting-cell">
                                                <QueueBubbles waiting={instance.waiting} compact />
                                            </TableCell>
                                        </TableRow>
                                    ))}
                                </TableBody>
                            </Table>
                        ) : (
                            <EmptyState
                                title="No matching instances"
                                description="Try another instance ID or state."
                                action="Clear filters"
                                onReset={() => {
                                    setQuery("")
                                    setStatus("all")
                                }}
                            />
                        )}
                    </div>
                    <p className="la-observer-connection-note">Connections are active WebSocket connections, not unique people.</p>
                </>
            ) : (
                <p className="la-observer-instance-empty" role="status">
                    No {actorName} instances have been created yet.
                </p>
            )}
        </section>
    )
}

function QueueWait({ stats }: { stats?: QueueWaitStats }) {
    if (!stats) return <span title="No admitted requests in the last hour">—</span>
    return (
        <span className="la-observer-wait" title={`Average of ${stats.admitted.toLocaleString()} admitted requests in the last hour; longest wait ${formatWait(stats.maxMs)}`}>
            {formatWait(stats.averageMs)}
            <small>max {formatWait(stats.maxMs)}</small>
        </span>
    )
}

function InstanceQueueWait({ stats }: { stats?: QueueWaitStats }) {
    return (
        <section className="la-observer-queue-wait-detail" aria-label="Queue wait">
            <h3>Queue wait</h3>
            <p>{stats ? "Time requests spent waiting to enter this actor over the last hour." : "No requests were admitted to this actor in the last hour."}</p>
            {stats && (
                <dl className="la-observer-summary la-observer-summary-compact">
                    <div>
                        <dt>Average</dt>
                        <dd aria-label="Average queue wait">{formatWait(stats.averageMs)}</dd>
                        <p>Per admitted request</p>
                    </div>
                    <div>
                        <dt>Longest</dt>
                        <dd aria-label="Longest queue wait">{formatWait(stats.maxMs)}</dd>
                        <p>Single request</p>
                    </div>
                    <div>
                        <dt>Admitted</dt>
                        <dd aria-label="Admitted requests">{stats.admitted.toLocaleString()}</dd>
                        <p>Requests that entered the actor</p>
                    </div>
                </dl>
            )}
        </section>
    )
}

function WaitingRequests({ waiting }: { waiting: ActorInstance["waiting"] }) {
    return (
        <section className="la-observer-waiting" aria-label="Waiting requests">
            <h3>Waiting requests{waiting != null && ` (${waiting.length})`}</h3>
            <p>{waiting == null ? "This host has not reported its waiting line." : waiting.length ? "Next to run first. Operations leave this line when they start." : "No requests waiting."}</p>
            {!!waiting?.length && <QueueBubbles waiting={waiting} />}
        </section>
    )
}

function QueueBubbles({ waiting, compact = false }: { waiting: ActorInstance["waiting"]; compact?: boolean }) {
    if (waiting == null) return <span title="Queue reporting is unavailable">Unavailable</span>
    if (!waiting.length) return <span className="la-observer-queue-empty">None</span>
    const visible = compact ? waiting.slice(0, 3) : waiting
    return (
        <ol className={`la-observer-queue${compact ? " la-observer-queue-compact" : ""}`} aria-label={`${waiting.length} waiting requests, next to run first`}>
            {visible.map((request, index) => (
                <li key={request.id}>
                    <Badge variant="outline" className="la-observer-queue-bubble" title={request.operation}>
                        {!compact && <span className="la-observer-queue-position">{index + 1}</span>}
                        <span className="la-observer-queue-operation">{request.operation}</span>
                    </Badge>
                </li>
            ))}
            {compact && waiting.length > visible.length && (
                <li>
                    <Badge aria-label={`${waiting.length - visible.length} more waiting requests`}>+{waiting.length - visible.length}</Badge>
                </li>
            )}
        </ol>
    )
}

function WebSocketConnections({ id, actorId, connections }: { id: string; actorId: string; connections: ActorInstance["connections"] }) {
    return (
        <section id={id} className="la-observer-connections" aria-labelledby={`${id}-heading`}>
            <div className="la-observer-instances-heading">
                <div>
                    <h3 id={`${id}-heading`}>
                        {actorId} WebSockets <Badge aria-hidden="true">{connections.length.toLocaleString()}</Badge>
                    </h3>
                    <p>{`${connections.length.toLocaleString()} active ${connections.length === 1 ? "connection" : "connections"}`}</p>
                </div>
            </div>
            {connections.length ? (
                <div className="la-observer-table-frame">
                    <Table aria-label={`${actorId} WebSocket connections`} className="la-observer-connection-table">
                        <TableHeader>
                            <TableRow>
                                <TableHead scope="col">Socket</TableHead>
                                <TableHead scope="col">Metadata</TableHead>
                            </TableRow>
                        </TableHeader>
                        <TableBody>
                            {connections.map(connection => (
                                <TableRow key={connection.id}>
                                    <TableCell className="la-observer-socket-id">{connection.id}</TableCell>
                                    <TableCell>
                                        <pre>{JSON.stringify(connection.metadata, null, 2)}</pre>
                                    </TableCell>
                                </TableRow>
                            ))}
                        </TableBody>
                    </Table>
                </div>
            ) : (
                <div className="la-observer-no-connections" role="status">
                    <Unplug aria-hidden="true" />
                    <p>No active WebSocket connections.</p>
                </div>
            )}
        </section>
    )
}

function SearchField({ label, placeholder, value, onChange }: { label: string; placeholder: string; value: string; onChange: (value: string) => void }) {
    return (
        <div className="la-observer-search">
            <Search aria-hidden="true" />
            <Input type="search" aria-label={label} placeholder={placeholder} value={value} onInput={event => onChange(event.currentTarget.value)} />
        </div>
    )
}

function EmptyState({ title, description, action, onReset }: { title: string; description: string; action?: string; onReset?: () => void }) {
    return (
        <div className="la-observer-empty">
            <Search aria-hidden="true" />
            <h3>{title}</h3>
            <p>{description}</p>
            {action && (
                <Button type="button" variant="outline" onClick={onReset}>
                    {action}
                </Button>
            )}
        </div>
    )
}

function matches(value: string, query: string) {
    return value.toLocaleLowerCase().includes(query.trim().toLocaleLowerCase())
}
function labelStatus(status: ActorInstance["status"]): string {
    return status[0]!.toUpperCase() + status.slice(1)
}

export { ActorObserver }
export type { ActorObserverProps }
