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

/** When the instance was live, and so held its CPU and memory reservation: 1 when live, 0 when dormant. */
export function LiveCard({ live, periods, failed, timeWindow }: LiveCardProps) {
    const loading = (!periods || live === null) && !failed
    const series = liveIntervals(periods ?? [], timeWindow)
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
            describePoint={index => ({ at: bucketLabel(timeWindow, index), value: series[index] ? "Live" : "Dormant" })}
        >
            {failed && <TrendStatsSkeleton failed />}
        </TrendCard>
    )
}

/** 1 for each bucket the instance was live at any point in, like the other charts' per-bucket peaks. */
function liveIntervals(periods: ResidencyPeriod[], timeWindow: TimeWindow): number[] {
    return Array.from({ length: bucketCount(timeWindow) }, (_, index) => {
        const start = timeWindow.fromMs + index * timeWindow.stepMs
        const end = bucketEnd(timeWindow, index)
        return periods.some(period => period.fromMs < end && period.toMs > start) ? 1 : 0
    })
}
