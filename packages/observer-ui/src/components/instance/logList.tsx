import { Fragment, type ReactNode, useEffect, useRef, useState } from "react"

import { Check, ChevronRight, Copy, ListFilter } from "lucide-react"

import { formatLogClock, formatLogTime, formatMs, formatPlural } from "../../format.js"
import { type ParsedMessage, constructorRequestId, parseLogMessage, shortRequestId, summarizeJson } from "../../lib/logs.js"
import { cn } from "../../lib/utils.js"
import type { ActorLog, LogLevel } from "../../types/Logs.js"
import type { RequestTrace } from "../../types/Requests.js"
import { Button } from "../ui/button.js"
import { Skeleton } from "../ui/skeleton.js"

type Json = NonNullable<ParsedMessage["json"]> | string | number | boolean | null

export type LogColumns = { level: boolean; request: boolean }

type Layout = { columns: LogColumns; gridTemplateColumns: string; messageOffset: number }

const rowGrid = "grid items-center gap-x-4"
const columnGap = 16
const widths = { time: 104, level: 58, request: 104 }
const badge: Record<LogLevel, string> = {
    debug: "border-border/60 text-muted-foreground/70",
    info: "text-muted-foreground",
    warn: "border-warning/35 bg-warning/10 text-warning",
    error: "border-danger/40 bg-danger/15 text-danger"
}
const rowTint: Record<LogLevel, string> = {
    debug: "",
    info: "",
    warn: "shadow-[inset_2px_0_0_color-mix(in_oklab,var(--warning)_75%,transparent)]",
    error: "bg-[linear-gradient(90deg,color-mix(in_oklab,var(--danger)_10%,transparent),transparent_70%)] shadow-[inset_2px_0_0_var(--danger)]"
}
const kindLabels: Record<RequestTrace["kind"], string> = { method: "RPC", websocket: "WebSocket" }

export type LogListProps = {
    lines: ActorLog[] | null
    failed: boolean
    search: string
    calls: Map<string, RequestTrace>
    filtered: boolean
    onFilterRequest: (requestId: string) => void
    onClearFilters: () => void
    onReachEnd: (() => void) | undefined
    columns: LogColumns
}

export function LogList({ lines, failed, search, calls, filtered, onFilterRequest, onClearFilters, onReachEnd, columns }: LogListProps) {
    const layout = layoutFor(columns)
    const [expanded, setExpanded] = useState<ReadonlySet<string>>(new Set())
    const scroller = useRef<HTMLDivElement>(null)
    const end = useRef<HTMLDivElement>(null)
    useEffect(() => {
        if (!onReachEnd || !end.current) return
        const observer = new IntersectionObserver(entries => entries.some(entry => entry.isIntersecting) && onReachEnd(), { root: scroller.current, rootMargin: "240px" })
        observer.observe(end.current)
        return () => observer.disconnect()
    }, [onReachEnd])
    if (failed) return <p className="py-6 text-[13px] text-muted-foreground">Couldn't reach the observer API.</p>
    if (!lines) return <ListSkeleton layout={layout} />
    if (!lines.length) return <EmptyList filtered={filtered} onClearFilters={onClearFilters} />
    const toggle = (logId: string) =>
        setExpanded(open => {
            const next = new Set(open)
            if (!next.delete(logId)) next.add(logId)
            return next
        })
    return (
        <div ref={scroller} className="-mx-2 max-h-[640px] overflow-y-auto px-2">
            <div className={`${rowGrid} sticky top-0 z-[1] -mx-2 h-7.5 border-b bg-card px-4 text-xs text-muted-foreground`} style={{ gridTemplateColumns: layout.gridTemplateColumns }}>
                <span>Time</span>
                {columns.level && <span>Level</span>}
                {columns.request && <span>Request</span>}
                <span>Message</span>
            </div>
            <ol>
                {lines.map(line => (
                    <LogRow
                        key={line.logId}
                        line={line}
                        layout={layout}
                        expanded={expanded.has(line.logId)}
                        onToggle={() => toggle(line.logId)}
                        search={search}
                        call={line.requestId ? calls.get(line.requestId) : undefined}
                        onFilterRequest={onFilterRequest}
                    />
                ))}
            </ol>
            <div ref={end} aria-hidden className="h-px" />
        </div>
    )
}

function layoutFor(columns: LogColumns): Layout {
    const fixed = [widths.time, ...(columns.level ? [widths.level] : []), ...(columns.request ? [widths.request] : [])]
    return {
        columns,
        gridTemplateColumns: `${fixed.map(width => `${width}px`).join(" ")} minmax(0,1fr) 16px`,
        messageOffset: fixed.reduce((offset, width) => offset + width + columnGap, 0)
    }
}

function ListSkeleton({ layout }: { layout: Layout }) {
    return (
        <div className="py-2">
            {Array.from({ length: 6 }, (_, index) => (
                <div key={index} className={`${rowGrid} h-8.5 px-2`} style={{ gridTemplateColumns: layout.gridTemplateColumns }}>
                    <Skeleton className="h-3.5 w-20" />
                    {layout.columns.level && <Skeleton className="h-4 w-11" />}
                    {layout.columns.request && <Skeleton className="h-4 w-16" />}
                    <Skeleton className="h-3.5" style={{ width: `${35 + ((index * 23) % 50)}%` }} />
                </div>
            ))}
        </div>
    )
}

function EmptyList({ filtered, onClearFilters }: { filtered: boolean; onClearFilters: () => void }) {
    return (
        <p className="py-8 text-center text-[13px] text-muted-foreground">
            {filtered ? (
                <>
                    No lines match these filters.{" "}
                    <button type="button" onClick={onClearFilters} className="font-medium text-foreground underline decoration-border underline-offset-3 hover:decoration-foreground">
                        Clear filters
                    </button>
                </>
            ) : (
                "No logs in this time range."
            )}
        </p>
    )
}

type LogRowProps = {
    line: ActorLog
    layout: Layout
    expanded: boolean
    onToggle: () => void
    search: string
    call: RequestTrace | undefined
    onFilterRequest: (requestId: string) => void
}

function LogRow({ line, layout, expanded, onToggle, search, call, onFilterRequest }: LogRowProps) {
    const parsed = parseLogMessage(line.message)
    return (
        <li className={cn("-mx-2 border-b border-border/55 px-2 last:border-b-0", rowTint[line.level])}>
            <div onClick={onToggle} className={`${rowGrid} min-h-8.5 cursor-pointer text-[12.5px]`} style={{ gridTemplateColumns: layout.gridTemplateColumns }}>
                <span className="font-mono text-xs text-muted-foreground tabular-nums">{formatLogClock(line.timestampMs)}</span>
                {layout.columns.level && (
                    <span className={cn("justify-self-start rounded-sm border px-1.5 py-px text-[9.5px] font-bold tracking-[0.07em] uppercase", badge[line.level])}>{line.level}</span>
                )}
                {layout.columns.request && <RequestCell requestId={line.requestId} onFilter={onFilterRequest} />}
                <MessagePreview parsed={parsed} level={line.level} search={search} />
                <button
                    type="button"
                    aria-expanded={expanded}
                    aria-label={expanded ? "Collapse line" : "Expand line"}
                    onClick={event => {
                        event.stopPropagation()
                        onToggle()
                    }}
                    className="rounded-sm text-muted-foreground outline-none hover:text-foreground focus-visible:ring-2 focus-visible:ring-ring"
                >
                    <ChevronRight className={cn("size-3.5 transition-transform", expanded && "rotate-90")} />
                </button>
            </div>
            {expanded && <LogDetail line={line} parsed={parsed} search={search} call={call} indent={layout.messageOffset} onFilterRequest={onFilterRequest} />}
        </li>
    )
}

function RequestCell({ requestId, onFilter }: { requestId: string | null; onFilter: (requestId: string) => void }) {
    if (requestId === null)
        return (
            <span className="font-mono text-muted-foreground" title="Logged outside a request, for example by a timer that fired after its call returned">
                —
            </span>
        )
    if (requestId === constructorRequestId)
        return (
            <span className="justify-self-start rounded-sm border border-dashed px-1.5 text-[11px] text-muted-foreground" title="Logged by the constructor while the actor was loading">
                constructor
            </span>
        )
    return (
        <button
            type="button"
            title={`Show only request ${requestId}`}
            onClick={event => {
                event.stopPropagation()
                onFilter(requestId)
            }}
            className="max-w-full justify-self-start truncate rounded-sm bg-chart-2/12 px-1.5 py-px font-mono text-[11.5px] text-chart-2 outline-none hover:bg-chart-2/22 focus-visible:ring-2 focus-visible:ring-ring"
        >
            {shortRequestId(requestId)}
        </button>
    )
}

function MessagePreview({ parsed, level, search }: { parsed: ParsedMessage; level: LogLevel; search: string }) {
    if (parsed.json) return <JsonPreview value={parsed.json} search={search} />
    return (
        <span className={cn("flex min-w-0 items-center gap-2 font-mono", level === "debug" ? "text-muted-foreground" : "text-foreground/95")}>
            <span className="truncate">{highlight(parsed.firstLine, search)}</span>
            {parsed.moreLines > 0 && <span className="shrink-0 rounded-full border px-2 font-sans text-[11px] text-muted-foreground">+{formatPlural(parsed.moreLines, "line")}</span>}
        </span>
    )
}

function JsonPreview({ value, search }: { value: NonNullable<ParsedMessage["json"]>; search: string }) {
    const { headline, fields } = summarizeJson(value)
    return (
        <span className="flex min-w-0 items-center gap-2 font-mono">
            <span className="shrink-0 rounded-sm border border-chart-2/40 px-1 font-sans text-[9px] font-bold tracking-[0.07em] text-chart-2">JSON</span>
            <span className="truncate">
                {headline && <span className="mr-2.5 font-medium text-foreground">{highlight(headline, search)}</span>}
                {fields.map(([key, field], index) => (
                    <Fragment key={key}>
                        {index > 0 && <span className="text-muted-foreground"> · </span>}
                        <span className="text-muted-foreground">{key} </span>
                        <span className="text-foreground/95">{highlight(field, search)}</span>
                    </Fragment>
                ))}
            </span>
        </span>
    )
}

type LogDetailProps = { line: ActorLog; parsed: ParsedMessage; search: string; call: RequestTrace | undefined; indent: number; onFilterRequest: (requestId: string) => void }

function LogDetail({ line, parsed, search, call, indent, onFilterRequest }: LogDetailProps) {
    const requestId = line.requestId !== constructorRequestId ? line.requestId : null
    return (
        <div className="pr-6 pb-4" style={{ paddingLeft: indent }}>
            {parsed.json ? <JsonBlock value={parsed.json} /> : <MessageBlock message={line.message} level={line.level} search={search} />}
            <dl className="mt-3.5 grid grid-cols-2 gap-x-6 gap-y-3 text-xs">
                <Detail label="Time">{formatLogTime(line.timestampMs)}</Detail>
                <Detail label="Request">
                    {requestId ? <span className="font-mono">{requestId}</span> : line.requestId ? "Constructor, while the actor was loading" : "None; logged after its call returned"}
                </Detail>
                {call && (
                    <Detail label="Call">
                        {call.operation}
                        <span className="text-muted-foreground">
                            {" "}
                            · {kindLabels[call.kind]} · {call.outcome} after {formatMs(call.durationMs)}
                        </span>
                    </Detail>
                )}
                <Detail label="Actor">
                    {line.actorName} / {line.actorId}
                </Detail>
            </dl>
            <div className="mt-3.5 flex gap-2">
                <CopyButton text={line.message} />
                {requestId && (
                    <Button variant="outline" size="xs" onClick={() => onFilterRequest(requestId)}>
                        <ListFilter />
                        Show only this request
                    </Button>
                )}
            </div>
        </div>
    )
}

function MessageBlock({ message, level, search }: { message: string; level: LogLevel; search: string }) {
    const [first = "", ...rest] = message.split("\n")
    return (
        <pre className="max-h-96 overflow-auto rounded-lg border bg-(--code-bg) px-3.5 py-2.5 font-mono text-xs leading-relaxed break-words whitespace-pre-wrap text-foreground/95">
            <span className={cn(level === "error" && "text-danger")}>{highlight(first, search)}</span>
            {rest.length > 0 && `\n${rest.join("\n")}`}
        </pre>
    )
}

function JsonBlock({ value }: { value: Json }) {
    return (
        <div className="max-h-96 overflow-auto rounded-lg border bg-(--code-bg) px-3.5 py-2.5 font-mono text-xs leading-relaxed">
            <JsonNode value={value} />
        </div>
    )
}

function JsonNode({ value }: { value: Json }): ReactNode {
    if (value === null || typeof value !== "object") return <span className={typeof value === "string" ? "text-(--tk-string)" : "text-(--tk-type)"}>{JSON.stringify(value)}</span>
    const entries: [string | null, Json][] = Array.isArray(value) ? value.map(item => [null, item]) : Object.entries(value)
    const [open, close] = Array.isArray(value) ? ["[", "]"] : ["{", "}"]
    if (!entries.length) return <span className="text-muted-foreground">{`${open}${close}`}</span>
    return (
        <>
            <span className="text-muted-foreground">{open}</span>
            {entries.map(([key, item], index) => (
                <div key={key ?? index} className="pl-4">
                    {key !== null && <span className="text-(--tk-plain)">{key}: </span>}
                    <JsonNode value={item} />
                    {index < entries.length - 1 && <span className="text-muted-foreground">,</span>}
                </div>
            ))}
            <span className="text-muted-foreground">{close}</span>
        </>
    )
}

function Detail({ label, children }: { label: string; children: ReactNode }) {
    return (
        <div className="min-w-0">
            <dt className="mb-0.5 text-muted-foreground">{label}</dt>
            <dd className="truncate text-foreground/95">{children}</dd>
        </div>
    )
}

function CopyButton({ text }: { text: string }) {
    const [copied, setCopied] = useState(false)
    return (
        <Button
            variant="outline"
            size="xs"
            onClick={() =>
                void navigator.clipboard.writeText(text).then(() => {
                    setCopied(true)
                    setTimeout(() => setCopied(false), 1500)
                })
            }
        >
            {copied ? <Check /> : <Copy />}
            {copied ? "Copied" : "Copy message"}
        </Button>
    )
}

function highlight(text: string, search: string): ReactNode {
    if (!search) return text
    const lower = text.toLowerCase()
    const needle = search.toLowerCase()
    const parts: ReactNode[] = []
    let at = 0
    for (let index = lower.indexOf(needle); index >= 0; index = lower.indexOf(needle, at)) {
        parts.push(
            text.slice(at, index),
            <mark key={index} className="rounded-[2px] bg-primary/25 text-foreground shadow-[inset_0_-1px_0_var(--primary)]">
                {text.slice(index, index + needle.length)}
            </mark>
        )
        at = index + needle.length
    }
    parts.push(text.slice(at))
    return parts
}
