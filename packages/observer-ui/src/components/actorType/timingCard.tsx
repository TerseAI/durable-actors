import { formatMs, formatMsParts } from "../../format.js"
import { type TimingPick, timingStats } from "../../lib/requestStats.js"
import { bucketCount, bucketLabel, windowTicks } from "../../lib/timeRange.js"
import type { RequestTrace } from "../../types/Requests.js"
import type { TimeWindow } from "../../types/TimeRange.js"
import { Skeleton } from "../ui/skeleton.js"
import { TrendCard } from "../ui/trendCard.js"

export type TimingCardProps = {
    label: string
    pick: TimingPick
    color: string
    chartLabel: string
    requests: RequestTrace[] | null
    failed: boolean
    timeWindow: TimeWindow
}

export function TimingCard({ label, pick, color, chartLabel, requests, failed, timeWindow }: TimingCardProps) {
    const stats = requests && timingStats(requests, timeWindow, pick)
    const loading = !stats && !failed
    const [average, unit] = formatMsParts(stats?.average ?? 0)
    const value = stats ? average : loading ? <Skeleton className="h-11 w-20" /> : "—"
    const series = stats?.series ?? Array<number>(bucketCount(timeWindow)).fill(0)
    return (
        <TrendCard
            label={label}
            value={value}
            unit={unit}
            busy={loading}
            failed={!stats && failed}
            series={series}
            axis={windowTicks(timeWindow, 2)}
            color={color}
            chartLabel={chartLabel}
            describePoint={index => ({ at: bucketLabel(timeWindow, index), value: formatMs(series[index] ?? 0) })}
        />
    )
}
