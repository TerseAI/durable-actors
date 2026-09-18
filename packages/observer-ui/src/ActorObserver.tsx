import { useId, useState } from "react"

import { Box, ChevronRight, CircleHelp, RefreshCw, Search, Unplug } from "lucide-react"

import type { ActorInstance, ActorInventory, ObserverClient } from "./client.js"
import { Badge } from "./components/ui/badge.js"
import { Button } from "./components/ui/button.js"
import { Input } from "./components/ui/input.js"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "./components/ui/table.js"
import { useInventory } from "./observer-hooks.js"

interface ActorObserverProps {
    client: ObserverClient
    className?: string
    initialActorType?: string
}

function ActorObserver({ client, className = "", initialActorType }: ActorObserverProps) {
    const { inventory, loading, failed, retry } = useInventory(client)
    return (
        <section className={`la-observer ${className}`} aria-label="Actor observer">
            <div className="la-observer-toolbar">
                <div>
                    <div className="la-observer-title">
                        <h1>Actors</h1>
                    </div>
                    <p>Inspect instances, residency, and connections.</p>
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
                    <InventorySummary inventory={inventory} />
                    {inventory.actors.length ? (
                        <ActorTable inventory={inventory} initialActorType={initialActorType} />
                    ) : (
                        <EmptyState title="No actors yet" description="Deploy your actor classes to see them here. Instance counts appear as actors are used." />
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

function InventorySummary({ inventory }: { inventory: ActorInventory }) {
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
                <p>
                    {inventory.actors.length} actor {inventory.actors.length === 1 ? "type" : "types"}
                </p>
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
        </dl>
    )
}

function ActorTable({ inventory, initialActorType }: { inventory: ActorInventory; initialActorType?: string }) {
    const [query, setQuery] = useState("")
    const [selectedActorType, setSelectedActorType] = useState<string | undefined>(initialActorType)
    const detailsId = useId()
    const hasUnknown = inventory.actors.some(actor => actor.unknown > 0)
    const actors = inventory.actors.filter(actor => matches(actor.actorType, query))
    const selectedActor = actors.find(actor => actor.actorType === selectedActorType)
    return (
        <>
            <div className="la-observer-list-toolbar">
                <h2>
                    Actor types <Badge>{inventory.actors.length}</Badge>
                </h2>
                <SearchField label="Search actors" placeholder="Search actors…" value={query} onChange={setQuery} />
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
                            </TableRow>
                        </TableHeader>
                        <TableBody>
                            {actors.map(actor => (
                                <TableRow key={actor.actorType} data-state={selectedActorType === actor.actorType ? "selected" : undefined}>
                                    <TableCell className="la-observer-name">
                                        <Disclosure
                                            expanded={selectedActorType === actor.actorType}
                                            controls={detailsId}
                                            onClick={() => setSelectedActorType(current => (current === actor.actorType ? undefined : actor.actorType))}
                                        >
                                            <Box className="la-observer-type-icon" aria-hidden="true" />
                                            <span>{actor.actorType}</span>
                                        </Disclosure>
                                    </TableCell>
                                    <TableCell>
                                        <span className={actor.live > 0 ? "la-observer-live" : undefined}>{actor.live.toLocaleString()}</span>
                                    </TableCell>
                                    <TableCell>{actor.dormant.toLocaleString()}</TableCell>
                                    {hasUnknown && <TableCell>{actor.unknown.toLocaleString()}</TableCell>}
                                    <TableCell>{(actor.live + actor.dormant + actor.unknown).toLocaleString()}</TableCell>
                                </TableRow>
                            ))}
                        </TableBody>
                    </Table>
                ) : (
                    <EmptyState title="No matching actors" description="Try a different actor name." action="Clear search" onReset={() => setQuery("")} />
                )}
            </div>
            {hasUnknown && (
                <p className="la-observer-unknown">
                    <CircleHelp aria-hidden="true" />
                    Some hosts have not reported which instances are loaded. Their counts are shown as unknown.
                </p>
            )}
            {selectedActor && <ActorInstances key={selectedActor.actorType} id={detailsId} actorType={selectedActor.actorType} instances={selectedActor.instances} />}
        </>
    )
}

function ActorInstances({ id, actorType, instances }: { id: string; actorType: string; instances: ActorInstance[] }) {
    const [query, setQuery] = useState("")
    const [status, setStatus] = useState("all")
    const [selectedInstanceId, setSelectedInstanceId] = useState<string>()
    const connectionDetailsId = `${id}-connections`
    const filtered = instances.filter(instance => matches(instance.actorId, query) && (status === "all" || instance.status === status))
    const selectedInstance = filtered.find(instance => instance.actorId === selectedInstanceId)
    return (
        <section id={id} className="la-observer-instances" aria-labelledby={`${id}-heading`}>
            <div className="la-observer-instances-heading">
                <div>
                    <h2 id={`${id}-heading`}>
                        {actorType} instances <Badge aria-hidden="true">{instances.length.toLocaleString()}</Badge>
                    </h2>
                    <p>Inspect an instance to view its active WebSocket connections.</p>
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
                            <Table aria-label={`${actorType} instances`} className="la-observer-instance-table">
                                <TableHeader>
                                    <TableRow>
                                        <TableHead scope="col">Instance</TableHead>
                                        <TableHead scope="col">State</TableHead>
                                        <TableHead scope="col">Connections</TableHead>
                                    </TableRow>
                                </TableHeader>
                                <TableBody>
                                    {filtered.map(instance => (
                                        <TableRow key={instance.actorId} data-state={selectedInstanceId === instance.actorId ? "selected" : undefined}>
                                            <TableCell className="la-observer-instance-id">
                                                <Disclosure
                                                    expanded={selectedInstanceId === instance.actorId}
                                                    controls={connectionDetailsId}
                                                    onClick={() => setSelectedInstanceId(current => (current === instance.actorId ? undefined : instance.actorId))}
                                                >
                                                    <span>{instance.actorId}</span>
                                                </Disclosure>
                                            </TableCell>
                                            <TableCell>
                                                <Badge variant="outline" className={`la-observer-status-${instance.status}`}>
                                                    <span className={`la-observer-dot la-observer-dot-${instance.status}`} />
                                                    {labelStatus(instance.status)}
                                                </Badge>
                                            </TableCell>
                                            <TableCell>{instance.connections.length.toLocaleString()}</TableCell>
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
                    {selectedInstance && <WebSocketConnections id={connectionDetailsId} actorId={selectedInstance.actorId} connections={selectedInstance.connections} />}
                </>
            ) : (
                <p className="la-observer-instance-empty" role="status">
                    No {actorType} instances have been created yet.
                </p>
            )}
        </section>
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

function Disclosure({ expanded, controls, onClick, children }: { expanded: boolean; controls: string; onClick: () => void; children: React.ReactNode }) {
    return (
        <Button variant="ghost" type="button" className="la-observer-actor-button" aria-expanded={expanded} aria-controls={expanded ? controls : undefined} onClick={onClick}>
            <ChevronRight className="la-observer-chevron" aria-hidden="true" />
            {children}
        </Button>
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
