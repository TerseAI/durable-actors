import { useState } from "react"

import { RefreshCw } from "lucide-react"

import type { ObserverClient } from "./client.js"
import { Button } from "./components/ui/button.js"
import { Input } from "./components/ui/input.js"
import { useInventory } from "./observer-hooks.js"

export function WebSocketObserver({ client }: { client: ObserverClient }) {
    const { inventory, failed, retry } = useInventory(client)
    const [query, setQuery] = useState("")
    const sockets =
        inventory?.actors.flatMap(actor => actor.instances.flatMap(instance => instance.connections.map(connection => ({ ...connection, actorName: actor.actorName, actorId: instance.actorId })))) ??
        []
    const visible = sockets.filter(socket => `${socket.id} ${socket.actorName} ${socket.actorId}`.toLocaleLowerCase().includes(query.trim().toLocaleLowerCase()))
    return (
        <section className="la-observer overview" aria-label="WebSocket observer">
            <div className="overview-heading">
                <div>
                    <h1>WebSockets</h1>
                    <p>{inventory ? `${sockets.length.toLocaleString()} active connections` : "Connecting to inventory…"}</p>
                </div>
                <Button variant="outline" onClick={retry}>
                    <RefreshCw aria-hidden="true" />
                    Refresh
                </Button>
            </div>
            {failed && (
                <p role="alert" className="overview-alert">
                    Could not refresh connections. {inventory ? "Showing the last received inventory; it may be out of date." : "Check your connection and access."} Retrying automatically.
                </p>
            )}
            <div className="overview-panel">
                <div className="overview-filterbar">
                    <Input aria-label="Filter connections" placeholder="Filter by connection, actor, or instance…" value={query} onInput={event => setQuery(event.currentTarget.value)} />
                </div>
                <div className="overview-table-scroll" role="region" aria-label="WebSocket connections" tabIndex={0}>
                    <table className="overview-table socket-table">
                        <thead>
                            <tr>
                                <th scope="col">Connection</th>
                                <th scope="col">Actor class</th>
                                <th scope="col">Instance</th>
                                <th scope="col">Metadata</th>
                            </tr>
                        </thead>
                        <tbody>
                            {visible.map(socket => (
                                <tr key={JSON.stringify([socket.actorName, socket.actorId, socket.id])}>
                                    <td>{socket.id}</td>
                                    <td>{socket.actorName}</td>
                                    <td>{socket.actorId}</td>
                                    <td>
                                        <details>
                                            <summary>View metadata</summary>
                                            <pre>{JSON.stringify(socket.metadata, null, 2)}</pre>
                                        </details>
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
                {!visible.length && (
                    <div className="overview-empty" role="status">
                        <strong>{!inventory ? (failed ? "Connections unavailable" : "Loading connections…") : sockets.length ? "No matching connections" : "No active WebSockets"}</strong>
                        <p>{sockets.length ? "Try another connection or actor identifier." : "Open an actor WebSocket to see its connection and metadata here."}</p>
                        {!!sockets.length && (
                            <Button variant="outline" onClick={() => setQuery("")}>
                                Clear filter
                            </Button>
                        )}
                    </div>
                )}
            </div>
            <div className="overview-data-scope">
                <p>Connections are active WebSockets, not unique people. Counts follow the latest host report.</p>
            </div>
        </section>
    )
}
