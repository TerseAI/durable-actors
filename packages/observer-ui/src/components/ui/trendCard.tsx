import type { ReactNode } from "react"

import { Skeleton } from "./skeleton.js"
import { type PointDescription, type Reference, Sparkline } from "./sparkline.js"
import { StatCard } from "./statCard.js"

export type TrendCardProps = {
    label: string
    /** A qualifier beside the label. The page header already shows the time range. */
    aside?: string
    value: ReactNode
    unit: ReactNode
    busy?: boolean
    series: number[]
    axis: string[]
    color: string
    chartLabel: string
    describePoint: (index: number) => PointDescription
    references?: Reference[]
    bounds?: [number, number]
    children?: ReactNode
}

export function TrendCard({ label, aside, value, unit, busy, series, axis, color, chartLabel, describePoint, references, bounds, children }: TrendCardProps) {
    return (
        <StatCard
            label={label}
            aside={aside && <span className="shrink-0 text-xs font-normal tabular-nums">{aside}</span>}
            value={value}
            unit={busy ? <Skeleton className="h-3.5 w-9" /> : unit}
            busy={busy}
        >
            <div className="mt-2 flex flex-wrap items-center gap-x-4.5 gap-y-1 text-[13px] text-muted-foreground tabular-nums">{children}</div>
            {busy ? (
                <TrendSkeleton ticks={axis.length} />
            ) : (
                <>
                    <Sparkline values={series} color={color} label={chartLabel} describePoint={describePoint} references={references} bounds={bounds} />
                    <div className="mt-1.5 flex justify-between text-xs text-muted-foreground tabular-nums">
                        {axis.map(tick => (
                            <span key={tick}>{tick}</span>
                        ))}
                    </div>
                </>
            )}
        </StatCard>
    )
}

function TrendSkeleton({ ticks }: { ticks: number }) {
    return (
        <>
            <Skeleton className="mt-auto h-16 w-full rounded-sm" />
            <div className="mt-1.5 flex h-4 items-center justify-between">
                {Array.from({ length: ticks }, (_, index) => (
                    <Skeleton key={index} className="h-3 w-9" />
                ))}
            </div>
        </>
    )
}

export function TrendStatsSkeleton({ failed }: { failed: boolean }) {
    if (failed) return <span>Couldn't reach the observer API.</span>
    return (
        <>
            <Skeleton className="h-3.5 w-20" />
            <Skeleton className="h-3.5 w-28" />
        </>
    )
}

export function TrendStat({ label, value, suffix }: { label: string; value: string; suffix?: string }) {
    return (
        <span>
            {label} <b className="font-bold text-foreground">{value}</b>
            {suffix && ` ${suffix}`}
        </span>
    )
}
