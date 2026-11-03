import { useData } from "../../client/DatatContext.js"
import { formatClock, formatDecimal } from "../../format.js"
import { bucketCount, bucketMs, emptySeries, trendAxis, trendWindowMs } from "../../lib/trendWindow.js"
import type { RequestTrace } from "../../types/Requests.js"
import type { RequestRateMetrics } from "../../types/Overview.js"
import { Skeleton } from "../ui/skeleton.js"

import { TrendCard, TrendStat } from "./trendCard.js"

export function RequestRateCard() {
    const { requests, isRequestsError } = useData()
    const now = Date.now()
    const metrics = requests && getMetrics(requests, now)
    const loading = !metrics && !isRequestsError
    const value = metrics ? formatDecimal(metrics.current) : loading ? <Skeleton className="h-11 w-28" /> : "—"
    return (
        <TrendCard
            label="Requests / Second"
            window={"last 1\u00a0h"}
            value={value}
            unit="req/s"
            busy={loading}
            series={metrics?.series ?? emptySeries}
            axis={trendAxis(now)}
            color="var(--chart-1)"
            chartLabel={metrics ? `Requests per second over the last hour, peaking at ${formatDecimal(metrics.peak)}` : "Requests per second over the last hour"}
        >
            {metrics ? <RateStats {...metrics} /> : <RateStatsSkeleton failed={isRequestsError} />}
        </TrendCard>
    )
}

function getMetrics(requests: RequestTrace[], now: number): RequestRateMetrics {
    const series = bucketRates(requests, now)
    const peak = Math.max(...series)
    return {
        current: series.at(-1) ?? 0,
        average: series.reduce((sum, rate) => sum + rate, 0) / bucketCount,
        peak,
        peakAt: formatClock(now - trendWindowMs + (series.indexOf(peak) + 1) * bucketMs),
        series
    }
}

function bucketRates(requests: RequestTrace[], now: number): number[] {
    const start = now - trendWindowMs
    const counts = Array<number>(bucketCount).fill(0)
    // Rerouted attempts are retried under the same request, so counting them would double-count.
    for (const { startedAtMs, outcome } of requests) {
        const index = Math.floor((startedAtMs - start) / bucketMs)
        if (outcome !== "rerouted" && index >= 0) counts[Math.min(index, bucketCount - 1)]! += 1
    }
    return counts.map(count => count / (bucketMs / 1000))
}

function RateStats({ average, peak, peakAt }: RequestRateMetrics) {
    return (
        <>
            <TrendStat label="Avg" value={formatDecimal(average)} />
            <TrendStat label="Peak" value={formatDecimal(peak)} suffix={`at ${peakAt}`} />
        </>
    )
}

function RateStatsSkeleton({ failed }: { failed: boolean }) {
    if (failed) return <span>Couldn't reach the observer API.</span>
    return (
        <>
            <Skeleton className="h-3.5 w-16" />
            <Skeleton className="h-3.5 w-28" />
        </>
    )
}
