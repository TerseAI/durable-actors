import type { ReactNode } from "react"

import { formatClock, formatCount, formatCpu, formatMebibytes, scaleBytes } from "../../format.js"
import { bucketMax } from "../../lib/resources.js"
import { bucketLabel, describeWindow, windowTicks } from "../../lib/timeRange.js"
import type { ActorKill, ActorResources, KillReason, ResourceLimits } from "../../types/Resources.js"
import type { TimeWindow } from "../../types/TimeRange.js"
import { Skeleton } from "../ui/skeleton.js"
import { TrendCard, TrendStat, TrendStatsSkeleton } from "../ui/trendCard.js"

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
    const growth = latest === undefined ? 0 : latest - (sizes[0]?.bytes ?? latest)
    const series = bucketMax(sizes, timeWindow, size => size.bytes, true)
    const shown = latest === undefined ? undefined : scaleBytes(latest)
    const describe = (bytes: number) => Object.values(scaleBytes(bytes)).join(" ")
    return (
        <ResourceCard label="SQLite Storage" shown={shown} describe={describe} series={series} color="var(--chart-4)" resources={resources} failed={failed} timeWindow={timeWindow}>
            {latest === undefined ? <span>No size reported in this window.</span> : <TrendStat label="Growth" value={`${growth < 0 ? "−" : "+"}${describe(Math.abs(growth))}`} />}
        </ResourceCard>
    )
}

export function MemoryCard({ resources, limits, failed, timeWindow }: LimitCardProps) {
    const samples = resources?.samples ?? []
    const peak = samples.length ? Math.max(...samples.map(sample => sample.memoryBytes)) : undefined
    const sampledLimit = samples.at(-1)?.memoryLimitBytes
    // Reported limits come first; a missing reported limit is uncapped.
    const limitMib = limits ? limits.memoryLimitMib : sampledLimit && sampledLimit / mebibyte
    const series = bucketMax(samples, timeWindow, sample => sample.memoryBytes)
    const describe = (bytes: number) => `${formatMebibytes(bytes)} MiB`
    return (
        <ResourceCard
            label="Peak Memory"
            shown={peak === undefined ? undefined : { value: formatMebibytes(peak), unit: "MiB" }}
            describe={describe}
            series={series}
            color="var(--chart-1)"
            resources={resources}
            failed={failed}
            timeWindow={timeWindow}
        >
            {peak === undefined && <span>{noSamples}</span>}
            {limits && <TrendStat label="Requested" value={`${formatCount(limits.memoryMib)} MiB`} />}
            {(limits || sampledLimit) && <TrendStat label="Limit" value={limitMib ? `${formatCount(limitMib)} MiB` : "None"} />}
            <Kills kills={resources?.kills ?? []} />
        </ResourceCard>
    )
}

export function CpuCard({ resources, limits, failed, timeWindow }: LimitCardProps) {
    const usage = (resources?.samples ?? []).flatMap(sample => (sample.cpuMillicores === null ? [] : [sample.cpuMillicores]))
    const average = usage.length ? usage.reduce((sum, value) => sum + value, 0) / usage.length : undefined
    const series = bucketMax(resources?.samples ?? [], timeWindow, sample => sample.cpuMillicores)
    // Samples are thousandths of a CPU; on GKE one CPU is one vCPU.
    const describe = (millicores: number) => `${formatCpu(millicores)} CPU = ${formatCount(Math.round(millicores))} millicores (1 CPU = 1 vCPU)`
    return (
        <ResourceCard
            label="Avg CPU"
            shown={average === undefined ? undefined : { value: formatCpu(average), unit: "CPU" }}
            describe={describe}
            series={series}
            color="var(--chart-3)"
            resources={resources}
            failed={failed}
            timeWindow={timeWindow}
        >
            {usage.length ? <TrendStat label="Peak" value={`${formatCpu(Math.max(...usage))} CPU`} /> : <span>{noSamples}</span>}
            {limits && <TrendStat label="Requested" value={`${formatCpu(limits.cpuMillis)} CPU`} />}
            {limits && <TrendStat label="Limit" value={limits.cpuLimitMillis ? `${formatCpu(limits.cpuLimitMillis)} CPU` : "None"} />}
        </ResourceCard>
    )
}

type CardProps = ResourceCardProps & {
    label: string
    shown: { value: string; unit: string } | undefined
    describe: (value: number) => string
    series: number[]
    color: string
    children: ReactNode
}

function ResourceCard({ label, shown, describe, series, color, resources, failed, timeWindow, children }: CardProps) {
    const loading = !resources && !failed
    const value = shown ? shown.value : loading ? <Skeleton className="h-11 w-20" /> : "—"
    return (
        <TrendCard
            label={label}
            window={describeWindow(timeWindow)}
            value={value}
            unit={shown?.unit ?? ""}
            busy={loading}
            series={series}
            axis={windowTicks(timeWindow, 2)}
            color={color}
            chartLabel={`${label}, ${describeWindow(timeWindow)}`}
            describePoint={index => ({ at: bucketLabel(timeWindow, index), value: describe(series[index] ?? 0) })}
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
