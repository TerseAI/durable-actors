import { useId } from "react"

import { useData } from "../../client/DatatContext.js"
import { formatCount, formatPercent } from "../../format.js"
import type { ActorInventory } from "../../types/Actors.js"
import { dormantTicks, liveTicks } from "../ui/residencyTicks.js"
import { Skeleton } from "../ui/skeleton.js"

type ActorTypeRow = {
    actorName: string
    total: number
    live: number
    dormant: number
}

export function ActorTypesCard() {
    const { actorTypes, isError } = useData()
    const titleId = useId()
    const rows = actorTypes && toRows(actorTypes)
    return (
        <section aria-labelledby={titleId} aria-busy={(!rows && !isError) || undefined} className="rounded-xl border bg-card px-5.5 pt-5 pb-2.5 text-card-foreground shadow-(--shadow-control)">
            <header className="mb-1.5 flex flex-wrap items-center justify-between gap-3">
                <h2 id={titleId} className="text-base font-bold tracking-tight">
                    Actor Types
                </h2>
                <ResidencyKey />
            </header>
            <ActorTypeLanes rows={rows} failed={isError} />
        </section>
    )
}

function toRows(inventory: ActorInventory): ActorTypeRow[] {
    return inventory.actors
        .map(({ actorName, instances }) => ({
            actorName,
            total: instances.length,
            live: instances.filter(instance => instance.status === "live").length,
            dormant: instances.filter(instance => instance.status === "dormant").length
        }))
        .sort((a, b) => b.total - a.total)
}

function plural(count: number, noun: string): string {
    return `${formatCount(count)} ${count === 1 ? noun : `${noun}s`}`
}

function ResidencyKey() {
    return (
        <div className="flex gap-4 text-xs text-muted-foreground">
            <span className="flex items-center gap-1.5">
                <span className="size-2 rounded-xs bg-success" />
                Live
            </span>
            <span className="flex items-center gap-1.5">
                <span className="size-2 rounded-xs bg-border" />
                Dormant
            </span>
        </div>
    )
}

function ActorTypeLanes({ rows, failed }: { rows: ActorTypeRow[] | null; failed: boolean }) {
    if (failed) return <p className="py-4 text-[13px] text-muted-foreground">Couldn't reach the observer API.</p>
    if (!rows) return <LaneSkeletons />
    if (!rows.length) return <p className="py-4 text-[13px] text-muted-foreground">No actor types deployed yet.</p>
    const largest = Math.max(...rows.map(row => row.total), 1)
    return (
        <ul>
            {rows.map(row => (
                <ActorTypeLane key={row.actorName} row={row} largest={largest} />
            ))}
        </ul>
    )
}

const laneClass = "grid items-center gap-x-7 gap-y-3 border-t border-border/60 py-4 first:border-t-0 md:grid-cols-[190px_1fr_168px]"

function ActorTypeLane({ row, largest }: { row: ActorTypeRow; largest: number }) {
    const liveShare = row.total ? row.live / row.total : 0
    return (
        <li className={`${laneClass} tabular-nums`}>
            <div className="min-w-0">
                <b translate="no" className="block truncate font-mono text-sm font-medium">
                    {row.actorName}
                </b>
                <span className="mt-0.75 block text-xs text-muted-foreground">{plural(row.total, "instance")}</span>
            </div>
            <ResidencyTrack row={row} liveShare={liveShare} width={row.total / largest} />
            <div className="grid grid-cols-2 gap-5 text-right">
                <div>
                    <b className="block text-lg leading-tight font-bold tracking-tight">{formatCount(row.live)}</b>
                    <span className="mt-0.75 block text-xs text-success">live · {formatPercent(liveShare)}</span>
                </div>
                <div>
                    <b className="block text-lg leading-tight font-medium tracking-tight text-foreground/85">{formatCount(row.dormant)}</b>
                    <span className="mt-0.75 block text-xs text-muted-foreground">dormant</span>
                </div>
            </div>
        </li>
    )
}

function ResidencyTrack({ row, liveShare, width }: { row: ActorTypeRow; liveShare: number; width: number }) {
    return (
        <div className="relative flex h-5.5 items-center before:absolute before:inset-x-0 before:top-1/2 before:h-px before:bg-border/60">
            <div
                role="img"
                aria-label={`${formatCount(row.live)} of ${formatCount(row.total)} live, ${formatPercent(liveShare)}`}
                className="relative flex h-5.5 min-w-2.5 overflow-hidden rounded-[3px]"
                style={{ width: `${width * 100}%` }}
            >
                <div className={liveTicks} style={{ width: `${liveShare * 100}%` }} />
                <div className={`flex-1 ${dormantTicks}`} />
            </div>
        </div>
    )
}

function LaneSkeletons() {
    return (
        <ul>
            {[100, 60, 20].map(width => (
                <li key={width} className={laneClass}>
                    <div>
                        <Skeleton className="h-4 w-28" />
                        <Skeleton className="mt-1.5 h-3 w-20" />
                    </div>
                    <Skeleton className="h-5.5 rounded-[3px]" style={{ width: `${width}%` }} />
                    <Skeleton className="ml-auto h-8 w-32" />
                </li>
            ))}
        </ul>
    )
}
