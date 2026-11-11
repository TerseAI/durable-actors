import type { ReactNode } from "react"

import { formatClock, formatCpu, formatMebibytes, scaleBytes } from "../../format.js"
import { bucketMax } from "../../lib/resources.js"
import { bucketLabel, describeWindow, windowTicks } from "../../lib/timeRange.js"
import { cn } from "../../lib/utils.js"
import type { ActorKill, ActorResources, KillReason, ResourceLimits } from "../../types/Resources.js"
import type { TimeWindow } from "../../types/TimeRange.js"
import { Skeleton } from "../ui/skeleton.js"
import type { Reference } from "../ui/sparkline.js"
import { TrendCard, TrendStatsSkeleton } from "../ui/trendCard.js"

export type ResourceCardProps = {
    resources: ActorResources | null
    failed: boolean
    timeWindow: TimeWindow
}

type LimitCardProps = ResourceCardProps & { limits: ResourceLimits | null }

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

export function MemoryCard({ resources, limits, failed, timeWindow }: LimitCardProps) {
    const samples = resources?.samples ?? []
    const latest = samples.at(-1)?.memoryBytes
    const sampledLimit = samples.at(-1)?.memoryLimitBytes ?? undefined
    // Reported limits come first; a missing reported limit is uncapped.
    const requested = limits && limits.memoryMib * mebibyte
    const limit = limits ? limits.memoryLimitMib && limits.memoryLimitMib * mebibyte : sampledLimit
    const megabytes = (bytes: number) => `${formatMebibytes(bytes)} MiB`
    const color = "var(--chart-1)"
    return (
        <ResourceCard
            label="Memory"
            shown={latest === undefined ? undefined : { value: formatMebibytes(latest), unit: "MiB" }}
            describe={bytes => `${megabytes(bytes)} used`}
            details={reservation(requested, limit, megabytes)}
            series={bucketMax(samples, timeWindow, sample => sample.memoryBytes)}
            references={references(requested, limit)}
            color={color}
            resources={resources}
            failed={failed}
            timeWindow={timeWindow}
        >
            {latest === undefined && <span>{noSamples}</span>}
            <Legend color={color} requested={requested} limit={limit} known={Boolean(limits || sampledLimit)} />
            <Kills kills={resources?.kills ?? []} />
        </ResourceCard>
    )
}

export function CpuCard({ resources, limits, failed, timeWindow }: LimitCardProps) {
    const samples = resources?.samples ?? []
    const latest = [...samples].reverse().find(sample => sample.cpuMillicores !== null)?.cpuMillicores ?? undefined
    const cpus = (millicores: number) => `${formatCpu(millicores)} CPU`
    const color = "var(--chart-3)"
    return (
        <ResourceCard
            label="CPU"
            shown={latest === undefined ? undefined : { value: formatCpu(latest), unit: "CPU" }}
            describe={millicores => `${cpus(millicores)} used`}
            details={reservation(limits?.cpuMillis, limits?.cpuLimitMillis, cpus)}
            series={bucketMax(samples, timeWindow, sample => sample.cpuMillicores)}
            references={references(limits?.cpuMillis, limits?.cpuLimitMillis)}
            color={color}
            resources={resources}
            failed={failed}
            timeWindow={timeWindow}
        >
            {latest === undefined && <span>{noSamples}</span>}
            <Legend color={color} requested={limits?.cpuMillis} limit={limits?.cpuLimitMillis} known={Boolean(limits)} />
        </ResourceCard>
    )
}

function references(requested: number | null | undefined, limit: number | null | undefined): Reference[] {
    return [...(requested ? [{ value: requested }] : []), ...(limit ? [{ value: limit, dotted: true }] : [])]
}

function reservation(requested: number | null | undefined, limit: number | null | undefined, format: (value: number) => string): string[] {
    return [...(requested ? [`Requested ${format(requested)}`] : []), ...(limit ? [`Limit ${format(limit)}`] : [])]
}

type CardProps = ResourceCardProps & {
    label: string
    shown: { value: string; unit: string } | undefined
    describe: (value: number) => string
    details?: string[]
    series: number[]
    references?: Reference[]
    color: string
    children: ReactNode
}

function ResourceCard({ label, shown, describe, details, series, references, color, resources, failed, timeWindow, children }: CardProps) {
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
            describePoint={index => ({ at: bucketLabel(timeWindow, index), value: describe(series[index] ?? 0), details })}
            references={references}
        >
            {resources ? children : <TrendStatsSkeleton failed={failed} />}
        </TrendCard>
    )
}

type LegendProps = { color: string; requested: number | null | undefined; limit: number | null | undefined; known: boolean }

/** Names the chart's lines: usage in color, the reservation solid grey, the limit dotted grey. Values are in the chart's hover. */
function Legend({ color, requested, limit, known }: LegendProps) {
    return (
        <>
            <Key label="Used" swatch={<i className="w-3 border-t-2" style={{ borderColor: color }} />} />
            {requested ? <Key label="Requested" swatch={<i className="w-3 border-t-2 border-muted-foreground" />} /> : null}
            {known && <Key label={limit ? "Limit" : "No limit"} swatch={<i className={cn("w-3 border-t-2 border-dotted border-muted-foreground", !limit && "opacity-40")} />} />}
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
