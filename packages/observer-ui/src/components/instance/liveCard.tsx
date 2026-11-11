import { formatPercent } from "../../format.js"
import { bucketCount, bucketEnd, bucketLabel, describeWindow, windowTicks } from "../../lib/timeRange.js"
import type { ResidencyPeriod } from "../../types/Residency.js"
import type { TimeWindow } from "../../types/TimeRange.js"
import { Skeleton } from "../ui/skeleton.js"
import { TrendCard, TrendStatsSkeleton } from "../ui/trendCard.js"

export type LiveCardProps = {
    live: boolean | null
    periods: ResidencyPeriod[] | null
    failed: boolean
    timeWindow: TimeWindow
}

/** When the instance was live, and so held its CPU and memory reservation, from 0 (dormant) to 1 (live). */
export function LiveCard({ live, periods, failed, timeWindow }: LiveCardProps) {
    const loading = (!periods || live === null) && !failed
    const series = liveShares(periods ?? [], timeWindow)
    const value = live === null ? loading ? <Skeleton className="h-11 w-28" /> : "—" : live ? "Live" : "Dormant"
    return (
        <TrendCard
            label="Live"
            value={value}
            unit=""
            busy={loading}
            series={series}
            bounds={[0, 1]}
            axis={windowTicks(timeWindow, 2)}
            color="var(--success)"
            chartLabel={`When the instance was live, ${describeWindow(timeWindow)}`}
            describePoint={index => ({ at: bucketLabel(timeWindow, index), value: describeShare(series[index] ?? 0) })}
        >
            {failed && <TrendStatsSkeleton failed />}
        </TrendCard>
    )
}

function describeShare(share: number): string {
    if (share >= 1) return "Live"
    if (share <= 0) return "Dormant"
    return `Live ${formatPercent(share)} of the time`
}

/** The share of each bucket the instance spent live. */
function liveShares(periods: ResidencyPeriod[], timeWindow: TimeWindow): number[] {
    return Array.from({ length: bucketCount(timeWindow) }, (_, index) => {
        const start = timeWindow.fromMs + index * timeWindow.stepMs
        const end = bucketEnd(timeWindow, index)
        const live = periods.reduce((sum, period) => sum + Math.max(0, Math.min(end, period.toMs) - Math.max(start, period.fromMs)), 0)
        return Math.min(1, live / Math.max(1, end - start))
    })
}
