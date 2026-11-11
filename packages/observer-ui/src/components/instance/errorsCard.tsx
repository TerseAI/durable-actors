import { formatCount } from "../../format.js"
import { isFailure, requestCounts } from "../../lib/requestStats.js"
import { bucketCount, bucketLabel, describeWindow, windowTicks } from "../../lib/timeRange.js"
import type { RequestTrace } from "../../types/Requests.js"
import type { TimeWindow } from "../../types/TimeRange.js"
import { Skeleton } from "../ui/skeleton.js"
import { TrendCard, TrendStatsSkeleton } from "../ui/trendCard.js"

export type ErrorsCardProps = {
    requests: RequestTrace[] | null
    failed: boolean
    timeWindow: TimeWindow
}

/** Failed, rejected and interrupted requests over the window. */
export function ErrorsCard({ requests, failed, timeWindow }: ErrorsCardProps) {
    const loading = !requests && !failed
    const totals = requests ? requestCounts(requests, timeWindow) : null
    const failures = requests ? requestCounts(requests.filter(isFailure), timeWindow) : null
    const total = failures?.reduce((sum, count) => sum + count, 0)
    const value = total !== undefined ? formatCount(total) : loading ? <Skeleton className="h-11 w-20" /> : "—"
    const series = failures ?? Array<number>(bucketCount(timeWindow)).fill(0)
    return (
        <TrendCard
            label="Errors"
            value={value}
            unit={total === 1 ? "failed request" : "failed requests"}
            busy={loading}
            series={series}
            axis={windowTicks(timeWindow, 2)}
            color="var(--danger)"
            chartLabel={`Failed requests, ${describeWindow(timeWindow)}`}
            describePoint={index => ({ at: bucketLabel(timeWindow, index), value: describeFailures(series[index] ?? 0, totals?.[index] ?? 0) })}
        >
            {!requests && <TrendStatsSkeleton failed={failed} />}
        </TrendCard>
    )
}

function describeFailures(failures: number, total: number): string {
    if (!total) return "No requests"
    return `${formatCount(failures)} of ${formatCount(total)} ${total === 1 ? "request" : "requests"} failed`
}
