import { formatDecimal } from "../../format.js"
import { requestRates } from "../../lib/requestStats.js"
import { bucketCount, bucketLabel, describeWindow, windowTicks } from "../../lib/timeRange.js"
import type { RequestRateMetrics } from "../../types/Overview.js"
import type { RequestTrace } from "../../types/Requests.js"
import type { TimeWindow } from "../../types/TimeRange.js"
import { Skeleton } from "../ui/skeleton.js"
import { TrendCard } from "../ui/trendCard.js"

export type RequestRateTrendProps = {
    requests: RequestTrace[] | null
    failed: boolean
    timeWindow: TimeWindow
    color: string
}

export function RequestRateTrend({ requests, failed, timeWindow, color }: RequestRateTrendProps) {
    const metrics = requests && getMetrics(requests, timeWindow)
    const loading = !metrics && !failed
    const value = metrics ? formatDecimal(metrics.current) : loading ? <Skeleton className="h-11 w-28" /> : "—"
    const series = metrics?.series ?? Array<number>(bucketCount(timeWindow)).fill(0)
    return (
        <TrendCard
            label="Requests / Second"
            value={value}
            unit="req/s"
            busy={loading}
            failed={!metrics && failed}
            series={series}
            axis={windowTicks(timeWindow, 2)}
            color={color}
            chartLabel={`Requests per second, ${describeWindow(timeWindow)}`}
            describePoint={index => ({ at: bucketLabel(timeWindow, index), value: `${formatDecimal(series[index] ?? 0)} req/s` })}
        />
    )
}

function getMetrics(requests: RequestTrace[], timeWindow: TimeWindow): RequestRateMetrics {
    const series = requestRates(requests, timeWindow)
    return { current: series.at(-1) ?? 0, series }
}
