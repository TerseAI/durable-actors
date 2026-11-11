import type { ReactNode } from "react"

import { formatClock, formatCores, formatMebibytes, formatMegabytes } from "../../format.js"
import { bucketMax } from "../../lib/resources.js"
import { bucketLabel, describeWindow, windowTicks } from "../../lib/timeRange.js"
import type { ActorKill, ActorResources, KillReason } from "../../types/Resources.js"
import type { TimeWindow } from "../../types/TimeRange.js"
import { Skeleton } from "../ui/skeleton.js"
import { TrendCard, TrendStat, TrendStatsSkeleton } from "../ui/trendCard.js"

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

export function StorageCard({ resources, failed, timeWindow }: ResourceCardProps) {
    const sizes = resources?.database ?? []
    const latest = sizes.at(-1)?.bytes
    const growth = latest === undefined ? 0 : latest - (sizes[0]?.bytes ?? latest)
    const series = bucketMax(sizes, timeWindow, size => size.bytes, true)
    return (
        <ResourceCard label="SQLite Storage" unit="MB" value={latest} format={formatMegabytes} series={series} color="var(--chart-4)" resources={resources} failed={failed} timeWindow={timeWindow}>
            {latest === undefined ? <span>No size reported in this window.</span> : <TrendStat label="Growth" value={`${growth < 0 ? "−" : "+"}${formatMegabytes(Math.abs(growth))} MB`} />}
        </ResourceCard>
    )
}

export function MemoryCard({ resources, failed, timeWindow }: ResourceCardProps) {
    const samples = resources?.samples ?? []
    const peak = samples.length ? Math.max(...samples.map(sample => sample.memoryBytes)) : undefined
    const limit = samples.at(-1)?.memoryLimitBytes
    const series = bucketMax(samples, timeWindow, sample => sample.memoryBytes)
    return (
        <ResourceCard label="Peak Memory" unit="MiB" value={peak} format={formatMebibytes} series={series} color="var(--chart-1)" resources={resources} failed={failed} timeWindow={timeWindow}>
            {peak === undefined && <span>{noSamples}</span>}
            {limit ? <TrendStat label="Limit" value={`${formatMebibytes(limit)} MiB`} /> : null}
            <Kills kills={resources?.kills ?? []} />
        </ResourceCard>
    )
}

export function CpuCard({ resources, failed, timeWindow }: ResourceCardProps) {
    const usage = (resources?.samples ?? []).flatMap(sample => (sample.cpuMillicores === null ? [] : [sample.cpuMillicores]))
    const average = usage.length ? usage.reduce((sum, value) => sum + value, 0) / usage.length : undefined
    const series = bucketMax(resources?.samples ?? [], timeWindow, sample => sample.cpuMillicores)
    return (
        <ResourceCard label="Avg CPU" unit="cores" value={average} format={formatCores} series={series} color="var(--chart-3)" resources={resources} failed={failed} timeWindow={timeWindow}>
            {usage.length ? <TrendStat label="Peak" value={`${formatCores(Math.max(...usage))} cores`} /> : <span>{noSamples}</span>}
        </ResourceCard>
    )
}

type CardProps = ResourceCardProps & {
    label: string
    unit: string
    value: number | undefined
    format: (value: number) => string
    series: number[]
    color: string
    children: ReactNode
}

function ResourceCard({ label, unit, value, format, series, color, resources, failed, timeWindow, children }: CardProps) {
    const loading = !resources && !failed
    const shown = value !== undefined ? format(value) : loading ? <Skeleton className="h-11 w-20" /> : "—"
    return (
        <TrendCard
            label={label}
            window={describeWindow(timeWindow)}
            value={shown}
            unit={unit}
            busy={loading}
            series={series}
            axis={windowTicks(timeWindow, 2)}
            color={color}
            chartLabel={`${label}, ${describeWindow(timeWindow)}`}
            describePoint={index => ({ at: bucketLabel(timeWindow, index), value: `${format(series[index] ?? 0)} ${unit}` })}
        >
            {resources ? children : <TrendStatsSkeleton failed={failed} />}
        </TrendCard>
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
