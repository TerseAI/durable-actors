import { formatCount, formatDuration, formatPreciseDateTime } from "../../format.js"
import type { Entry } from "../../lib/entries.js"
import { killLabels } from "../../lib/kills.js"
import type { EntryRecord } from "../../types/Entries.js"
import { Button } from "../ui/button.js"
import { DrawerDescription, DrawerHeader, DrawerTitle } from "../ui/drawer.js"
import { CopyJsonButton, JsonBlock } from "../ui/jsonView.js"

type DetailRecord = Exclude<EntryRecord, { type: "request" }>
type Fact = [label: string, value: string | null]

export function EntryDetails({ entry, record, onShowConnection }: { entry: Entry; record: DetailRecord; onShowConnection: (connectionId: string) => void }) {
    const { actorName, actorId } = record.item
    const facts: Fact[] = [
        ["Actor", actorName],
        ["Instance", actorId]
    ]
    const connectionId = "connectionId" in record.item ? record.item.connectionId : null
    const metadata = record.type === "connectionOpened" ? record.item.metadata : null
    return (
        <>
            <DrawerHeader className="shrink-0 gap-2 border-b px-5 py-4 pr-14">
                <DrawerTitle className="text-lg tracking-tight">{detailTitle(record)}</DrawerTitle>
                <DrawerDescription className="text-[13px] tabular-nums">
                    {entry.source} · {entry.level} · {formatPreciseDateTime(entry.atMs)}
                </DrawerDescription>
            </DrawerHeader>
            <div className="min-h-0 flex-1 overflow-y-auto px-5 py-4">
                {record.type === "log" && (
                    <pre className="mb-4 rounded-lg border bg-(--code-bg) px-3.5 py-2.5 font-mono text-xs leading-relaxed break-words whitespace-pre-wrap">{record.item.message}</pre>
                )}
                <dl className="border-t">
                    {[...facts, ...recordFacts(record)].map(
                        ([label, value]) =>
                            value !== null && (
                                <div key={label} className="grid grid-cols-[108px_minmax(0,1fr)] gap-3 border-b border-border/60 py-2.5">
                                    <dt className="text-xs text-muted-foreground">{label}</dt>
                                    <dd className="text-[13px] break-words text-foreground/90">{value}</dd>
                                </div>
                            )
                    )}
                </dl>
                {metadata != null && (
                    <section className="mt-4">
                        <h3 className="mb-2 text-sm font-medium">Metadata</h3>
                        <JsonBlock value={metadata} />
                        <div className="mt-2">
                            <CopyJsonButton value={metadata} />
                        </div>
                    </section>
                )}
                {connectionId && (
                    <Button variant="link" size="sm" className="mt-3 px-0" onClick={() => onShowConnection(connectionId)}>
                        View connection logs
                    </Button>
                )}
            </div>
        </>
    )
}

function detailTitle(record: DetailRecord): string {
    switch (record.type) {
        case "log":
            return "Console log"
        case "connectionOpened":
            return "Connection opened"
        case "connectionClosed":
            return "Connection closed"
        case "kill":
            return "Sandbox killed"
        case "lifecycle":
            return record.item.kind === "started" ? "Woke" : "Slept"
    }
}

function recordFacts(record: DetailRecord): Fact[] {
    switch (record.type) {
        case "log":
            return [
                ["Log ID", record.item.logId],
                ["Request ID", record.item.requestId]
            ]
        case "connectionOpened":
            return [
                ["Connection", record.item.connectionId],
                ["Gateway", record.item.gatewayId]
            ]
        case "connectionClosed": {
            const { connectionId, gatewayId, openedAtMs, closedAtMs, closeCode, closeReason, messagesIn, messagesOut } = record.item
            return [
                ["Connection", connectionId],
                ["Gateway", gatewayId],
                ["Opened", formatPreciseDateTime(openedAtMs)],
                ["Duration", formatDuration(closedAtMs - openedAtMs)],
                ["Close code", closeCode?.toString() ?? null],
                ["Close reason", closeReason],
                ["Messages in", messagesIn === null ? null : formatCount(messagesIn)],
                ["Messages out", messagesOut === null ? null : formatCount(messagesOut)]
            ]
        }
        case "kill":
            return [["Reason", killLabels[record.item.reason]]]
        case "lifecycle":
            return [["Time live", record.item.liveForMs === undefined ? null : formatDuration(record.item.liveForMs)]]
    }
}
