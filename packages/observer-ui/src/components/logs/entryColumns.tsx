import type { ReactNode } from "react"

import { formatDuration, formatLogClock, formatMs, formatPlural } from "../../format.js"
import type { Entry } from "../../lib/entries.js"
import { killLabels } from "../../lib/kills.js"
import { constructorRequestId, parseLogMessage, shortRequestId } from "../../lib/logs.js"
import type { QueryKey } from "../../lib/query.js"
import { cn } from "../../lib/utils.js"
import type { EntryRecord, EntrySource } from "../../types/Entries.js"
import type { ActorLifecycle } from "../../types/Events.js"
import type { LogLevel } from "../../types/Logs.js"
import { JsonSummary } from "../ui/jsonView.js"
import { Tag, type Tone, highlight } from "../ui/recordList.js"
import type { Column } from "../ui/recordTable.js"

const sourceTags: Record<EntrySource, { label: string; tone: Tone }> = {
    console: { label: "console", tone: "faint" },
    rpc: { label: "rpc", tone: "muted" },
    websocket: { label: "websocket", tone: "muted" },
    system: { label: "system", tone: "muted" }
}
const levelTones: Record<LogLevel, Tone> = { debug: "faint", info: "muted", warn: "warn", error: "danger" }

export type EntryColumnOptions = {
    search: string
    /** A class's log names the instance of each entry. */
    showInstance: boolean
    /** Narrows the search to entries with this value, as clicking a value in a row does. */
    onFilter: OnFilter
}

type OnFilter = (key: QueryKey, value: string) => void

export function entryColumns({ search, showInstance, onFilter }: EntryColumnOptions): Column<Entry>[] {
    const columns: Column<Entry>[] = [
        { id: "time", label: "Time", width: "104px", render: entry => <span className="font-mono text-xs text-muted-foreground tabular-nums">{formatLogClock(entry.atMs)}</span> },
        { id: "source", label: "Source", width: "88px", hideable: true, render: entry => <Tag tone={sourceTags[entry.source].tone}>{sourceTags[entry.source].label}</Tag> },
        { id: "level", label: "Level", width: "58px", hideable: true, render: entry => <Tag tone={levelTones[entry.level]}>{entry.level}</Tag> },
        { id: "request", label: "Request", width: "104px", hideable: true, hiddenByDefault: true, render: entry => <RequestCell record={entry.record} search={search} onFilter={onFilter} /> },
        { id: "message", label: "Message", width: "minmax(0,1fr)", render: entry => <EntryMessage record={entry.record} search={search} onFilter={onFilter} /> },
        {
            id: "duration",
            label: "Duration",
            width: "72px",
            align: "right",
            hideable: true,
            title: "How long a request or connection lasted, queue wait included",
            render: entry => <span className="text-muted-foreground tabular-nums">{durationOf(entry.record)}</span>
        }
    ]
    if (showInstance)
        columns.splice(3, 0, {
            id: "instance",
            label: "Instance",
            width: "120px",
            hideable: true,
            render: entry => (
                <IdChip title={`Show only instance ${entry.record.item.actorId}`} onClick={() => onFilter("instance", entry.record.item.actorId)}>
                    {highlight(entry.record.item.actorId, search)}
                </IdChip>
            )
        })
    return columns
}

function EntryMessage({ record, search, onFilter }: { record: EntryRecord; search: string; onFilter: OnFilter }) {
    switch (record.type) {
        case "request": {
            const { operation } = record.item
            return (
                <FilterText title={`Show only ${operation} requests`} onClick={() => onFilter("operation", operation)} className="max-w-full truncate font-mono text-foreground">
                    {highlight(operation, search)}
                </FilterText>
            )
        }
        case "log":
            return <LogMessage message={record.item.message} level={record.item.level} search={search} />
        case "connectionOpened":
            return <Sentence>Connection opened</Sentence>
        case "connectionClosed":
            return <Sentence>Connection closed</Sentence>
        case "kill":
            return <Sentence>Sandbox killed: {highlight(killLabels[record.item.reason].toLowerCase(), search)}</Sentence>
        case "lifecycle":
            return <Sentence>{lifecycleMessage(record.item)}</Sentence>
    }
}

function lifecycleMessage({ kind, liveForMs }: ActorLifecycle): string {
    if (kind === "started") return "Woke"
    return liveForMs === undefined ? "Slept" : `Slept after ${formatDuration(liveForMs)} live`
}

function Sentence({ children }: { children: ReactNode }) {
    return <span className="flex min-w-0 items-center gap-2 text-foreground/95">{children}</span>
}

function LogMessage({ message, level, search }: { message: string; level: LogLevel; search: string }) {
    const parsed = parseLogMessage(message)
    if (parsed.json) return <JsonSummary value={parsed.json} search={search} tagged />
    return (
        <span className={cn("flex min-w-0 items-center gap-2 font-mono", level === "debug" ? "text-muted-foreground" : "text-foreground/95")}>
            <span className="truncate">{highlight(parsed.firstLine, search)}</span>
            {parsed.moreLines > 0 && <span className="shrink-0 rounded-full border px-2 font-sans text-[11px] text-muted-foreground">+{formatPlural(parsed.moreLines, "line")}</span>}
        </span>
    )
}

function RequestCell({ record, search, onFilter }: { record: EntryRecord; search: string; onFilter: OnFilter }) {
    if (record.type !== "request" && record.type !== "log") return <span className="text-muted-foreground">—</span>
    const { requestId } = record.item
    if (requestId === null)
        return (
            <span className="font-mono text-muted-foreground" title="Logged outside a request, for example by a timer that fired after its call returned">
                —
            </span>
        )
    if (requestId === constructorRequestId)
        return (
            <span className="rounded-sm border border-dashed px-1.5 text-[11px] text-muted-foreground" title="Logged by the constructor while the actor was loading">
                constructor
            </span>
        )
    return (
        <IdChip title={`Show only request ${requestId}`} onClick={() => onFilter("request", requestId)}>
            {highlight(shortRequestId(requestId), search)}
        </IdChip>
    )
}

function IdChip({ title, onClick, children }: { title: string; onClick: () => void; children: ReactNode }) {
    return (
        <button
            type="button"
            title={title}
            onClick={event => {
                event.stopPropagation()
                onClick()
            }}
            className="max-w-full shrink-0 truncate rounded-sm bg-chart-2/12 px-1.5 py-px font-mono text-[11.5px] text-chart-2 outline-none hover:bg-chart-2/22 focus-visible:ring-2 focus-visible:ring-ring"
        >
            {children}
        </button>
    )
}

// A value inside a message that narrows the list to rows with the same value.
function FilterText({ title, onClick, className, children }: { title: string; onClick: () => void; className: string; children: ReactNode }) {
    return (
        <button
            type="button"
            title={title}
            translate="no"
            onClick={event => {
                event.stopPropagation()
                onClick()
            }}
            className={cn("rounded-sm text-left underline decoration-transparent underline-offset-3 outline-none hover:decoration-current focus-visible:ring-2 focus-visible:ring-ring", className)}
        >
            {children}
        </button>
    )
}

function durationOf(record: EntryRecord): string {
    if (record.type === "request") return formatMs(record.item.durationMs)
    if (record.type === "connectionClosed") return formatDuration(record.item.closedAtMs - record.item.openedAtMs)
    return ""
}
