import type { ReactNode } from "react"

import { Sparkline } from "../ui/sparkline.js"
import { StatCard } from "../ui/statCard.js"

export type TrendCardProps = {
    label: string
    window: string
    value: string
    unit: string
    series: number[]
    axis: string[]
    color: string
    chartLabel: string
    children: ReactNode
}

export function TrendCard({ label, window, value, unit, series, axis, color, chartLabel, children }: TrendCardProps) {
    return (
        <StatCard label={label} aside={<span className="shrink-0 text-xs font-normal tabular-nums">{window}</span>} value={value} unit={unit}>
            <div className="mt-2 flex flex-wrap items-center gap-x-4.5 gap-y-1 text-[13px] text-muted-foreground tabular-nums">{children}</div>
            <Sparkline values={series} color={color} label={chartLabel} />
            <div className="mt-1.5 flex justify-between text-xs text-muted-foreground tabular-nums">
                {axis.map(tick => (
                    <span key={tick}>{tick}</span>
                ))}
            </div>
        </StatCard>
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
