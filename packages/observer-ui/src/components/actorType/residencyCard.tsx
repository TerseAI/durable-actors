import { useState } from "react"

import { formatClock, formatCount } from "../../format.js"
import { byLastActive } from "../../lib/actorTypes.js"
import { trendWindowMs } from "../../lib/trendWindow.js"
import { cn } from "../../lib/utils.js"
import type { ActorInstance, ActorType } from "../../types/Actors.js"
import type { ResidencyHistory, ResidencyPeriod } from "../../types/Residency.js"
import { SectionCard } from "../ui/sectionCard.js"
import { Skeleton } from "../ui/skeleton.js"

import { InstanceStatus } from "./instanceStatus.js"

const previewRows = 10
const tickCount = 6
const rowGrid = "grid grid-cols-[minmax(0,210px)_1fr_minmax(0,128px)] items-center gap-6"

type Segment = { left: number; width: number; now: boolean }

export type ResidencyCardProps = {
    actor: ActorType | null
    history: ResidencyHistory | null
    failed: boolean
    now: number
}

export function ResidencyCard({ actor, history, failed, now }: ResidencyCardProps) {
    const [expanded, setExpanded] = useState(false)
    const instances = actor && byLastActive(actor.instances)
    const livePeriods = history && groupLivePeriods(history)
    const shown = expanded ? instances : instances?.slice(0, previewRows)
    return (
        <SectionCard title="Residency" aside={<ResidencyKeys />} busy={!shown && !failed} className="pb-3">
            {failed ? (
                <p className="py-4 text-[13px] text-muted-foreground">Couldn't reach the observer API.</p>
            ) : (
                <>
                    <div className="mt-3.5">
                        <TimeAxis now={now} />
                        {shown && livePeriods ? (
                            shown.map(instance => <ResidencyRow key={instance.actorId} instance={instance} periods={livePeriods.get(instance.actorId) ?? []} now={now} />)
                        ) : (
                            <RowSkeletons />
                        )}
                    </div>
                    {instances && <ResidencyFooter shown={shown?.length ?? 0} total={instances.length} expanded={expanded} onToggle={() => setExpanded(!expanded)} />}
                </>
            )}
        </SectionCard>
    )
}

function groupLivePeriods({ periods }: ResidencyHistory): Map<string, ResidencyPeriod[]> {
    const grouped = new Map<string, ResidencyPeriod[]>()
    for (const period of periods) {
        if (period.status === "live") grouped.set(period.actorId, [...(grouped.get(period.actorId) ?? []), period])
    }
    return grouped
}

function ResidencyKeys() {
    return (
        <div className="flex gap-4.5 text-xs text-muted-foreground">
            <span className="flex items-center gap-1.75">
                <span className="h-2 w-4 rounded-full bg-success" />
                Live
            </span>
            <span className="flex items-center gap-1.75">
                <span className="w-4 border-t border-dashed border-muted-foreground/60" />
                Dormant
            </span>
        </div>
    )
}

function TimeAxis({ now }: { now: number }) {
    const start = now - trendWindowMs
    return (
        <div aria-hidden className={`${rowGrid} h-6 border-b`}>
            <span className="text-xs text-muted-foreground">Instance</span>
            <div className="relative h-full font-mono text-[11px] text-muted-foreground tabular-nums">
                {Array.from({ length: tickCount + 1 }, (_, index) => (
                    <span
                        key={index}
                        className={cn("absolute top-1", index === 0 ? "" : index === tickCount ? "-translate-x-full text-foreground" : "-translate-x-1/2")}
                        style={{ left: `${(index / tickCount) * 100}%` }}
                    >
                        {index === tickCount ? "Now" : formatClock(start + (index / tickCount) * trendWindowMs)}
                    </span>
                ))}
            </div>
            <span className="text-right text-xs text-muted-foreground">Time live</span>
        </div>
    )
}

function ResidencyRow({ instance, periods, now }: { instance: ActorInstance; periods: ResidencyPeriod[]; now: number }) {
    const segments = toSegments(periods, now)
    const minutes = Math.round(segments.reduce((sum, segment) => sum + segment.width, 0) * (trendWindowMs / 60_000))
    const live = instance.status === "live"
    return (
        <div className={`${rowGrid} h-11.5 border-b border-border/50 last:border-b-0`}>
            <div className="min-w-0">
                <b translate="no" className="block truncate font-mono text-[13px] font-medium">
                    {instance.actorId}
                </b>
                <InstanceStatus live={live} className="mt-0.5 text-[11px]">
                    {live ? "Live now" : sleptLabel(periods, instance)}
                </InstanceStatus>
            </div>
            <ResidencyTrack segments={segments} label={`${instance.actorId}: ${minutes ? `${formatCount(minutes)} minutes live` : "dormant all hour"} in the last hour`} />
            <span className={cn("text-right text-[13px] tabular-nums", minutes ? "text-foreground/85" : "text-muted-foreground")}>{formatCount(minutes)}&nbsp;min</span>
        </div>
    )
}

function toSegments(periods: ResidencyPeriod[], now: number): Segment[] {
    const start = now - trendWindowMs
    return periods
        .map(period => ({ fromMs: Math.max(period.fromMs, start), toMs: Math.min(period.toMs, now) }))
        .filter(period => period.toMs > period.fromMs)
        .map(period => ({ left: (period.fromMs - start) / trendWindowMs, width: (period.toMs - period.fromMs) / trendWindowMs, now: now - period.toMs < 1000 }))
}

function sleptLabel(periods: ResidencyPeriod[], instance: ActorInstance): string {
    const lastLive = periods.at(-1)?.toMs ?? instance.lastActiveAtMs
    return lastLive ? `Slept ${formatClock(lastLive)}` : "Dormant"
}

const trackGrid = "bg-[linear-gradient(90deg,color-mix(in_oklab,var(--border)_55%,transparent)_1px,transparent_1px)] bg-size-[calc(100%/6)_100%] shadow-[inset_-1px_0_0_var(--border)]"

function ResidencyTrack({ segments, label }: { segments: Segment[]; label: string }) {
    return (
        <div role="img" aria-label={label} className={`relative h-full ${trackGrid} before:absolute before:inset-x-0 before:top-1/2 before:border-t before:border-dashed before:border-border`}>
            {segments.map(segment => (
                <span
                    key={segment.left}
                    className={cn(
                        "absolute top-1/2 h-2.5 -translate-y-1/2 rounded-full bg-success shadow-[0_0_12px_color-mix(in_oklab,var(--success)_20%,transparent)]",
                        segment.now && "rounded-r-[1px] after:absolute after:top-0.75 after:right-1 after:size-1 after:rounded-full after:bg-background"
                    )}
                    style={{ left: `${segment.left * 100}%`, width: `${segment.width * 100}%` }}
                />
            ))}
        </div>
    )
}

function ResidencyFooter({ shown, total, expanded, onToggle }: { shown: number; total: number; expanded: boolean; onToggle: () => void }) {
    return (
        <div className="flex justify-between border-t pt-3 text-xs text-muted-foreground tabular-nums">
            <span>
                Showing {formatCount(shown)} of {formatCount(total)} instances
            </span>
            {total > previewRows && (
                <button type="button" onClick={onToggle} className="font-medium text-primary hover:underline">
                    {expanded ? "Show Fewer" : "Show All Instances"}
                </button>
            )}
        </div>
    )
}

function RowSkeletons() {
    return Array.from({ length: 4 }, (_, index) => (
        <div key={index} className={`${rowGrid} h-11.5 border-b border-border/50 last:border-b-0`}>
            <div>
                <Skeleton className="h-3.5 w-32" />
                <Skeleton className="mt-1.5 h-2.5 w-16" />
            </div>
            <Skeleton className="h-2.5 rounded-full" style={{ width: `${70 - index * 15}%` }} />
            <Skeleton className="ml-auto h-3.5 w-12" />
        </div>
    ))
}
