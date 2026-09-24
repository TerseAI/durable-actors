import { useEffect, useState } from "react"
import { JsonView } from "react-json-view-lite"

import { Download, RefreshCw } from "lucide-react"

import type { ObserverClient } from "./client.js"
import { Button } from "./components/ui/button.js"
import { Input } from "./components/ui/input.js"
import { NativeSelect, NativeSelectOption } from "./components/ui/native-select.js"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "./components/ui/table.js"
import type { StateRecord, StateSnapshot } from "./state-data.js"
import { stateDiff } from "./state-diff.js"
import { useStateInspection } from "./state-inspection.js"

type Focus = { requestId?: string; connectionId?: string }
interface Props {
    client: ObserverClient
    actorName: string
    actorId: string
    onInspectRequest?: (focus: Focus) => void
}

export function StateInspector(props: Props) {
    const { client, actorName, actorId } = props
    const data = useStateInspection(client, actorName, actorId)
    const [tab, setTab] = useState<"state" | "changes">("state")
    const [selected, setSelected] = useState<number>()
    const [baseline, setBaseline] = useState<number>()
    const [query, setQuery] = useState("")
    const [revision, setRevision] = useState(0)
    const snapshot = data.current?.snapshot
    const latest = snapshot?.stateVersion ?? 0
    const version = selected ?? latest
    const waiting = data.knownVersion > latest || latest > (data.history?.records[0]?.stateVersion ?? 0)
    return (
        <section className="la-state" aria-label="Persisted state">
            <div className="la-state-heading">
                <div>
                    <h3>Persisted state</h3>
                    <p>Committed fields and their progression in storage.</p>
                </div>
                <div className="la-state-actions">
                    {snapshot && (
                        <Button variant="outline" size="sm" onClick={() => download(snapshot.state, `${actorName}-${actorId}-v${latest}.json`)}>
                            <Download aria-hidden="true" />
                            Download JSON
                        </Button>
                    )}
                    <Button
                        variant="outline"
                        size="sm"
                        disabled={data.loading}
                        onClick={() => {
                            data.refresh()
                            setRevision(value => value + 1)
                        }}
                        aria-label="Refresh state"
                    >
                        <RefreshCw aria-hidden="true" />
                        Refresh
                    </Button>
                </div>
            </div>
            <div className="la-state-tabs" aria-label="State views">
                <Button variant={tab === "state" ? "secondary" : "ghost"} aria-pressed={tab === "state"} onClick={() => setTab("state")}>
                    State
                </Button>
                <Button variant={tab === "changes" ? "secondary" : "ghost"} aria-pressed={tab === "changes"} onClick={() => setTab("changes")}>
                    Changes
                </Button>
                {snapshot && <span className="la-state-version">Version {latest}</span>}
            </div>
            {data.failed && <p role="alert">Could not refresh state. Check your connection and use Refresh to retry.{data.current && " Showing the last loaded version."}</p>}
            {waiting && (
                <p className="la-state-notice" role="status">
                    A newer commit is syncing to storage. Retrying automatically…
                </p>
            )}
            {!data.current && !data.failed && <p role="status">Loading persisted state…</p>}
            {data.current && !snapshot && <p className="la-state-empty">No persisted state yet. Fields appear after the actor commits its first change.</p>}
            {tab === "state" && snapshot && (
                <>
                    <Attribution record={snapshot} onInspect={props.onInspectRequest} />
                    <Input aria-label="Filter persisted fields" placeholder="Filter fields…" value={query} onChange={event => setQuery(event.target.value)} />
                    <StateFields state={snapshot.state} query={query} schema={data.current?.schema ?? null} />
                </>
            )}
            {tab === "changes" && (
                <div className="la-state-history">
                    <aside aria-label="State versions">
                        <h4>Versions</h4>
                        {!data.history?.records.length && <p>No recorded history yet.</p>}
                        <ol>
                            {data.history?.records.map(record => (
                                <li key={record.stateVersion}>
                                    <button
                                        type="button"
                                        aria-label={`Version ${record.stateVersion} · ${record.attribution?.operation ?? "Unknown operation"}`}
                                        aria-pressed={version === record.stateVersion}
                                        onClick={() => {
                                            setSelected(record.stateVersion)
                                            setBaseline(undefined)
                                        }}
                                    >
                                        <strong>
                                            Version {record.stateVersion}
                                            <span>{record.attribution?.operation ?? "Unknown operation"}</span>
                                        </strong>
                                        <small>{record.attribution ? new Date(record.attribution.committedAtMs).toLocaleTimeString() : "Time unavailable"}</small>
                                    </button>
                                </li>
                            ))}
                        </ol>
                        {data.history?.nextBefore && (
                            <Button variant="outline" size="sm" disabled={data.loading} onClick={data.loadOlder}>
                                Load older versions
                            </Button>
                        )}
                    </aside>
                    <div className="la-state-comparison">
                        {selected !== undefined && latest > selected && (
                            <Button
                                variant="outline"
                                size="sm"
                                onClick={() => {
                                    setSelected(undefined)
                                    setBaseline(undefined)
                                }}
                            >
                                View latest · version {latest}
                            </Button>
                        )}
                        {!!version && (
                            <>
                                <div className="la-state-compare-heading">
                                    <h4>Version {version}</h4>
                                    <NativeSelect aria-label="Compare with version" value={baseline ?? Math.max(0, version - 1)} onChange={event => setBaseline(Number(event.target.value))}>
                                        <NativeSelectOption value={Math.max(0, version - 1)}>{version === 1 ? "Before first commit" : `Previous · version ${version - 1}`}</NativeSelectOption>
                                        {data.history?.records
                                            .filter(record => record.stateVersion < version - 1)
                                            .map(record => (
                                                <NativeSelectOption key={record.stateVersion} value={record.stateVersion}>
                                                    Version {record.stateVersion}
                                                </NativeSelectOption>
                                            ))}
                                    </NativeSelect>
                                </div>
                                <VersionComparison key={`${actorName}/${actorId}/${version}/${baseline}/${revision}`} {...props} version={version} baseline={baseline ?? Math.max(0, version - 1)} />
                            </>
                        )}
                    </div>
                </div>
            )}
            <p className="la-state-footnote">Values come from actor storage. Request events signal new versions; history follows completed uploads.</p>
        </section>
    )
}

function StateFields({ state, query, schema }: { state: Record<string, unknown>; query: string; schema: Record<string, unknown> | null }) {
    const fields = Object.entries(state).filter(([name]) => name.toLowerCase().includes(query.toLowerCase()))
    return (
        <div className="la-observer-table-frame">
            <Table aria-label="Persisted fields">
                <TableHeader>
                    <TableRow>
                        <TableHead>Field</TableHead>
                        <TableHead>Type</TableHead>
                        <TableHead>Value</TableHead>
                    </TableRow>
                </TableHeader>
                <TableBody>
                    {fields.map(([name, value]) => (
                        <TableRow key={name}>
                            <TableCell>
                                <code>{name}</code>
                            </TableCell>
                            <TableCell>
                                <span className="la-state-type">{jsonType(value)}</span>
                                {declaredType(schema, name) && <small className="la-state-schema">{declaredType(schema, name)}</small>}
                            </TableCell>
                            <TableCell>
                                <JsonValue value={value} />
                            </TableCell>
                        </TableRow>
                    ))}
                </TableBody>
            </Table>
            {!fields.length && <p className="la-state-empty">{query ? "No matching fields." : "The persisted state is an empty object."}</p>}
        </div>
    )
}

function JsonValue({ value }: { value: unknown }) {
    const serialized = JSON.stringify(value)
    if (serialized.length > 100_000) return <span>Large value ({Math.round(serialized.length / 1024)} KB). Use Download JSON to inspect it.</span>
    return value !== null && typeof value === "object" ? (
        <JsonView data={value} shouldExpandNode={level => level < 1} style={jsonStyles} />
    ) : (
        <code className={`la-state-json-${jsonType(value)}`}>{serialized}</code>
    )
}

function Attribution({ record, onInspect }: { record: StateRecord; onInspect?: (focus: Focus) => void }) {
    const attribution = record.attribution
    return (
        <div className="la-state-attribution">
            <span>
                Committed by <strong>{attribution?.operation ?? "Unknown operation"}</strong>
            </span>
            <span>
                Request{" "}
                {onInspect ? (
                    <button type="button" onClick={() => onInspect({ requestId: record.requestId })}>
                        {record.requestId}
                    </button>
                ) : (
                    <code>{record.requestId}</code>
                )}
            </span>
            {attribution?.connectionId && (
                <span>
                    Connection{" "}
                    {onInspect ? (
                        <button type="button" onClick={() => onInspect({ connectionId: attribution.connectionId! })}>
                            {attribution.connectionId}
                        </button>
                    ) : (
                        <code>{attribution.connectionId}</code>
                    )}
                </span>
            )}
            {attribution && <time dateTime={new Date(attribution.committedAtMs).toISOString()}>{new Date(attribution.committedAtMs).toLocaleString()}</time>}
            {attribution?.interleaved && <p>Overlapping requests may contribute to this snapshot. This identifies the committing request.</p>}
        </div>
    )
}

function VersionComparison({ client, actorName, actorId, version, baseline, onInspectRequest }: Props & { version: number; baseline: number }) {
    const [result, setResult] = useState<{ after: StateSnapshot; html: string }>()
    const [error, setError] = useState<string>()
    const [downloadable, setDownloadable] = useState<StateSnapshot>()
    useEffect(() => {
        const controller = new AbortController()
        let worker: Worker | undefined
        async function load() {
            try {
                const [after, before] = await Promise.all([
                    client.getState!({ actorName, actorId, version }, controller.signal),
                    baseline ? client.getState!({ actorName, actorId, version: baseline }, controller.signal) : Promise.resolve({ snapshot: { state: {} } })
                ])
                if (controller.signal.aborted) return
                if (after.snapshot) setDownloadable(after.snapshot)
                if (!after.snapshot || !before.snapshot) throw new Error("This version is not available in storage. Its upload may be pending or its history may have expired. Refresh to retry.")
                const afterSnapshot = after.snapshot
                const size = JSON.stringify(before.snapshot.state).length + JSON.stringify(afterSnapshot.state).length
                if (size > 2_000_000) throw new Error("These snapshots are too large for an inline comparison. Inspect their JSON downloads instead.")
                if (size > 100_000 && typeof Worker !== "undefined") {
                    worker = new Worker(new URL("./state-diff.worker.ts", import.meta.url), { type: "module" })
                    worker.onmessage = event => {
                        if (event.data.error) setError(event.data.error)
                        else setResult({ after: afterSnapshot, html: event.data.html })
                    }
                    worker.onerror = () => setError("Could not compare these versions. Refresh to retry.")
                    worker.postMessage({ before: before.snapshot.state, after: afterSnapshot.state })
                } else setResult({ after: afterSnapshot, html: stateDiff(before.snapshot.state, afterSnapshot.state) })
            } catch (error) {
                if (!controller.signal.aborted) setError(error instanceof Error ? error.message : "Could not load versions.")
            }
        }
        void load()
        return () => {
            controller.abort()
            worker?.terminate()
        }
    }, [client, actorName, actorId, version, baseline])
    return (
        <>
            <p>{baseline ? `Comparing version ${baseline} → ${version}` : "Changes in the first persisted snapshot"}</p>
            {error && <p role="alert">{error}</p>}
            {!error && !result && <p role="status">Loading comparison…</p>}
            {downloadable && (
                <Button variant="ghost" size="sm" onClick={() => download(downloadable.state, `${actorName}-${actorId}-v${version}.json`)}>
                    Download version {version}
                </Button>
            )}
            {result && (
                <>
                    <Attribution record={result.after} onInspect={onInspectRequest} />
                    {result.html ? (
                        <div className="la-state-diff" aria-label="State changes" dangerouslySetInnerHTML={{ __html: result.html }} />
                    ) : (
                        <p>No persisted fields changed between these versions.</p>
                    )}
                </>
            )}
        </>
    )
}

function download(state: object, name: string) {
    const url = URL.createObjectURL(new Blob([JSON.stringify(state, null, 2)], { type: "application/json" }))
    const link = document.createElement("a")
    link.href = url
    link.download = name
    link.click()
    setTimeout(() => URL.revokeObjectURL(url), 1000)
}
function jsonType(value: unknown): string {
    return value === null ? "null" : Array.isArray(value) ? "array" : typeof value
}
function declaredType(schema: Record<string, unknown> | null, name: string): string | undefined {
    const definitions = schema?.definitions as Record<string, { type?: string | string[]; properties?: Record<string, { $ref?: string }> }> | undefined
    const ref = definitions?.State?.properties?.[name]?.$ref
    const type = ref && definitions?.[ref.split("/").pop()!.replace(/~1/g, "/").replace(/~0/g, "~")]?.type
    return type ? `Schema: ${Array.isArray(type) ? type.join(" | ") : type}` : undefined
}
const jsonStyles = {
    container: "la-state-json",
    basicChildStyle: "la-state-json-child",
    label: "la-state-json-label",
    clickableLabel: "la-state-json-label",
    nullValue: "la-state-json-null",
    undefinedValue: "la-state-json-null",
    numberValue: "la-state-json-number",
    stringValue: "la-state-json-string",
    booleanValue: "la-state-json-boolean",
    otherValue: "la-state-json-label",
    punctuation: "la-state-json-punctuation",
    expandIcon: "la-state-json-expand",
    collapseIcon: "la-state-json-collapse",
    collapsedContent: "la-state-json-collapsed",
    childFieldsContainer: "la-state-json-children"
}
