import type { ReactNode } from "react"

import { formatClock, formatCpu, formatMebibytes, scaleBytes } from "../../format.js"
import { bucketLatest, bucketMax } from "../../lib/resources.js"
import { bucketLabel, describeWindow, windowTicks } from "../../lib/timeRange.js"
import { cn } from "../../lib/utils.js"
import type { ActorKill, ActorResources, KillReason, ReservationPoint } from "../../types/Resources.js"
import type { TimeWindow } from "../../types/TimeRange.js"
import { Skeleton } from "../ui/skeleton.js"
import type { Reference } from "../ui/sparkline.js"
import { TrendCard, TrendStatsSkeleton } from "../ui/trendCard.js"

export type ResourceCardProps = {
    resources: ActorResources | null
    failed: boolean
    timeWindow: TimeWindow
}

const noSamples = "No samples in this window. Local development doesn't measure CPU or memory."

const killLabels: Record<KillReason, string> = {
    out_of_memory: "Out of memory",
    evicted: "Evicted",
    killed: "Killed"
}

const mebibyte = 1_048_576

export function StorageCard({ resources, failed, timeWindow }: ResourceCardProps) {
    const sizes = resources?.database ?? []
    const latest = sizes.at(-1)?.bytes
    const series = bucketMax(sizes, timeWindow, size => size.bytes, true)
    const describe = (bytes: number) => Object.values(scaleBytes(bytes)).join(" ")
    return (
        <ResourceCard
            label="SQLite Storage"
            shown={latest === undefined ? undefined : scaleBytes(latest)}
            describe={describe}
            series={series}
            color="var(--chart-4)"
            resources={resources}
            failed={failed}
            timeWindow={timeWindow}
        >
            {latest === undefined && <span>No size reported in this window.</span>}
        </ResourceCard>
    )
}

export function MemoryCard({ resources, failed, timeWindow }: ResourceCardProps) {
    const samples = resources?.samples ?? []
    const latest = samples.at(-1)?.memoryBytes
    const reserved = reservations(
        resources,
        timeWindow,
        point => point.memoryMib * mebibyte,
        point => (point.memoryLimitMib ?? 0) * mebibyte || null
    )
    const megabytes = (bytes: number) => `${formatMebibytes(bytes)} MiB`
    const color = "var(--chart-1)"
    return (
        <ResourceCard
            label="Memory"
            shown={latest === undefined ? undefined : { value: formatMebibytes(latest), unit: "MiB" }}
            describe={(bytes, index) => [`${megabytes(bytes)} used`, ...reserved.describe(index, megabytes)]}
            series={bucketMax(samples, timeWindow, sample => sample.memoryBytes)}
            references={reserved.references}
            color={color}
            resources={resources}
            failed={failed}
            timeWindow={timeWindow}
        >
            {latest === undefined && <span>{noSamples}</span>}
            <Legend color={color} {...reserved} />
            <Kills kills={resources?.kills ?? []} />
        </ResourceCard>
    )
}

export function CpuCard({ resources, failed, timeWindow }: ResourceCardProps) {
    const samples = resources?.samples ?? []
    const latest = [...samples].reverse().find(sample => sample.cpuMillicores !== null)?.cpuMillicores ?? undefined
    const reserved = reservations(
        resources,
        timeWindow,
        point => point.cpuMillis,
        point => point.cpuLimitMillis ?? null
    )
    const cpus = (millicores: number) => `${formatCpu(millicores)} CPU`
    const color = "var(--chart-3)"
    return (
        <ResourceCard
            label="CPU"
            shown={latest === undefined ? undefined : { value: formatCpu(latest), unit: "CPU" }}
            describe={(millicores, index) => [`${cpus(millicores)} used`, ...reserved.describe(index, cpus)]}
            series={bucketMax(samples, timeWindow, sample => sample.cpuMillicores)}
            references={reserved.references}
            color={color}
            resources={resources}
            failed={failed}
            timeWindow={timeWindow}
        >
            {latest === undefined && <span>{noSamples}</span>}
            <Legend color={color} {...reserved} />
        </ResourceCard>
    )
}

type Reserved = {
    requested: (number | null)[]
    limit: (number | null)[]
    known: boolean
    references: Reference[]
    describe: (index: number, format: (value: number) => string) => string[]
}

/** The request and limit in force at each point of the chart, as the actor's reports recorded them. */
function reservations(resources: ActorResources | null, timeWindow: TimeWindow, request: (point: ReservationPoint) => number, cap: (point: ReservationPoint) => number | null): Reserved {
    const points = resources?.reservations ?? []
    const requested = bucketLatest(points, timeWindow, request)
    const limit = bucketLatest(points, timeWindow, cap)
    return {
        requested,
        limit,
        known: points.length > 0,
        references: [{ values: requested }, { values: limit, dotted: true }],
        describe: (index, format) => {
            if (!points.length) return []
            const [atRequest, atLimit] = [requested[index], limit[index]]
            return [...(atRequest ? [`Requested ${format(atRequest)}`] : []), atLimit ? `Limit ${format(atLimit)}` : "No limit"]
        }
    }
}

type CardProps = ResourceCardProps & {
    label: string
    shown: { value: string; unit: string } | undefined
    /** The hover lines for one point, the value first. */
    describe: (value: number, index: number) => string | string[]
    series: number[]
    references?: Reference[]
    color: string
    children: ReactNode
}

function ResourceCard({ label, shown, describe, series, references, color, resources, failed, timeWindow, children }: CardProps) {
    const loading = !resources && !failed
    const value = shown ? shown.value : loading ? <Skeleton className="h-11 w-20" /> : "—"
    return (
        <TrendCard
            label={label}
            value={value}
            unit={shown?.unit ?? ""}
            busy={loading}
            series={series}
            axis={windowTicks(timeWindow, 2)}
            color={color}
            chartLabel={`${label}, ${describeWindow(timeWindow)}`}
            describePoint={index => {
                const [value = "", ...details] = [describe(series[index] ?? 0, index)].flat()
                return { at: bucketLabel(timeWindow, index), value, details }
            }}
            references={references}
        >
            {resources ? children : <TrendStatsSkeleton failed={failed} />}
        </TrendCard>
    )
}

/** Names the chart's lines: usage in color, the request solid grey, the limit dotted grey. Values are in the chart's hover. */
function Legend({ color, requested, limit, known }: { color: string } & Pick<Reserved, "requested" | "limit" | "known">) {
    const capped = limit.some(value => value !== null)
    return (
        <>
            <Key label="Used" swatch={<i className="w-3 border-t-2" style={{ borderColor: color }} />} />
            {requested.some(value => value !== null) && <Key label="Requested" swatch={<i className="w-3 border-t-2 border-muted-foreground" />} />}
            {known && <Key label={capped ? "Limit" : "No limit"} swatch={<i className={cn("w-3 border-t-2 border-dotted border-muted-foreground", !capped && "opacity-40")} />} />}
        </>
    )
}

function Key({ label, swatch }: { label: string; swatch: ReactNode }) {
    return (
        <span className="inline-flex items-center gap-1.5">
            <span aria-hidden className="inline-flex">
                {swatch}
            </span>
            {label}
        </span>
    )
}

function Kills({ kills }: { kills: ActorKill[] }) {
    const last = kills.at(-1)
    if (!last) return null
    return (
        <span className="text-destructive" title={last.message ?? undefined}>
            {killLabels[last.reason]} <b className="font-bold">{formatClock(last.atMs)}</b>
            {kills.length > 1 && ` +${kills.length - 1} more`}
        </span>
    )
}
