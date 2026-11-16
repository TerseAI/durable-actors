import { useState } from "react"

import { formatCount, formatPreciseDateTime } from "../../format.js"
import { useEventHistory } from "../../hooks/useEventHistory.js"
import { type InstanceEvent, instanceEvents } from "../../lib/instanceEvents.js"
import { cn } from "../../lib/utils.js"
import type { RecordedEvent } from "../../types/Events.js"
import type { LogLevel } from "../../types/Logs.js"
import type { ResidencyPeriod } from "../../types/Residency.js"
import type { TimeWindow } from "../../types/TimeRange.js"
import { ExpandFooter } from "../ui/expandFooter.js"
import { SectionCard } from "../ui/sectionCard.js"
import { SegmentedControl } from "../ui/segmentedControl.js"
import { Skeleton } from "../ui/skeleton.js"

import { levelFill } from "./logChart.js"
import { LevelBadge, levelTint } from "./logList.js"

type Kind = InstanceEvent["kind"]
type Shown = "all" | "warnings"

const previewRows = 10
const recorded: RecordedEvent["type"][] = ["kill"]
const rowGrid = "grid grid-cols-[128px_112px_minmax(0,1fr)] items-center gap-x-4"
const labels: Record<Kind, string> = { live: "Live", dormant: "Dormant", unknown: "Unknown", out_of_memory: "Out of memory", evicted: "Evicted", killed: "Killed" }
const levels: Record<Kind, LogLevel> = { live: "info", dormant: "info", unknown: "info", out_of_memory: "error", evicted: "warn", killed: "warn" }
const messages: Record<Kind, string> = {
    live: "Loaded into memory",
    dormant: "Unloaded from memory",
    unknown: "Stopped reporting its state",
    out_of_memory: "Sandbox used more memory than its limit",
    evicted: "Sandbox evicted by its node",
    killed: "Sandbox killed by a signal the platform didn't send"
}

export type EventsCardProps = {
    actorName: string
    actorId: string
    periods: ResidencyPeriod[] | null
    failed: boolean
    timeWindow: TimeWindow
}

export function EventsCard({ actorName, actorId, periods, failed, timeWindow }: EventsCardProps) {
    const [shown, setShown] = useState<Shown>("all")
    const [expanded, setExpanded] = useState(false)
    const { history, isError } = useEventHistory(recorded, timeWindow, actorName, actorId)
    const kills = history?.events.map(event => event.item)
    const events = periods && kills && instanceEvents(periods, kills, timeWindow)
    const warnings = events?.filter(event => event.kill)
    const listed = shown === "all" ? events : warnings
    const unreachable = failed || isError
    return (
        <SectionCard
            title="Events"
            description="Residency changes and sandbox kills."
            aside={<ShownControl value={shown} onChange={setShown} total={events?.length} warnings={warnings?.length} />}
            busy={!listed && !unreachable}
            className="pb-3"
        >
            {unreachable ? (
                <p className="py-4 text-[13px] text-muted-foreground">Couldn't reach the observer API.</p>
            ) : listed ? (
                <EventList events={listed} noun={shown === "all" ? "event" : "warning"} expanded={expanded} onToggle={() => setExpanded(!expanded)} />
            ) : (
                <RowSkeletons />
            )}
        </SectionCard>
    )
}

type ShownControlProps = { value: Shown; onChange: (value: Shown) => void; total: number | undefined; warnings: number | undefined }

function ShownControl({ value, onChange, total, warnings }: ShownControlProps) {
    const count = (events: number | undefined) => (events === undefined ? undefined : formatCount(events))
    return (
        <SegmentedControl
            label="Events shown"
            value={value}
            onChange={onChange}
            options={[
                { value: "all", label: "All", badge: count(total) },
                { value: "warnings", label: "Warnings", icon: <i className={cn("size-1.5 rounded-full", levelFill.warn)} />, badge: count(warnings) }
            ]}
        />
    )
}

type EventListProps = { events: InstanceEvent[]; noun: string; expanded: boolean; onToggle: () => void }

function EventList({ events, noun, expanded, onToggle }: EventListProps) {
    if (!events.length) return <p className="py-8 text-center text-[13px] text-muted-foreground">No {noun}s in this time range.</p>
    const rows = expanded ? events : events.slice(0, previewRows)
    return (
        <>
            <div className={`${rowGrid} mt-3.5 h-7.5 border-b px-2 text-xs text-muted-foreground`}>
                <span>Time</span>
                <span>Event</span>
                <span>Message</span>
            </div>
            <ol>
                {rows.map(event => (
                    <EventRow key={`${event.kind}:${event.atMs}`} event={event} />
                ))}
            </ol>
            {events.length > previewRows && <ExpandFooter shown={rows.length} total={events.length} noun={noun} expandLabel="Show All" expanded={expanded} onToggle={onToggle} />}
        </>
    )
}

function EventRow({ event }: { event: InstanceEvent }) {
    const level = levels[event.kind]
    return (
        <li className={cn(rowGrid, "min-h-8.5 border-b border-border/55 px-2 text-[12.5px] last:border-b-0", levelTint[level])}>
            <span className="font-mono text-xs text-muted-foreground tabular-nums">{formatPreciseDateTime(event.atMs)}</span>
            <LevelBadge level={level} className={cn(event.kind === "unknown" && "border-dashed")}>
                {labels[event.kind]}
            </LevelBadge>
            <span className={cn("min-w-0 py-1.5", event.kill ? "text-foreground/95" : "text-muted-foreground")}>{message(event)}</span>
        </li>
    )
}

function message({ kind, kill }: InstanceEvent): string {
    return kill?.message ? `${messages[kind]}: ${kill.message}.` : `${messages[kind]}.`
}

function RowSkeletons() {
    return (
        <div className="mt-3.5 py-2">
            {Array.from({ length: 4 }, (_, index) => (
                <div key={index} className={`${rowGrid} h-8.5 px-2`}>
                    <Skeleton className="h-3.5 w-24" />
                    <Skeleton className="h-4 w-16" />
                    <Skeleton className="h-3.5" style={{ width: `${40 + ((index * 23) % 40)}%` }} />
                </div>
            ))}
        </div>
    )
}
