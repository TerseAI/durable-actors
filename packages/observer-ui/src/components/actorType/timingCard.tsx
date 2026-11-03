import { formatCount, formatMs } from "../../format.js"
import { type TimingPick, timingStats } from "../../lib/requestStats.js"
import { emptySeries, trendAxis } from "../../lib/trendWindow.js"
import type { RequestTrace } from "../../types/Requests.js"
import { Skeleton } from "../ui/skeleton.js"
import { TrendCard, TrendStat, TrendStatsSkeleton } from "../ui/trendCard.js"

export type TimingCardProps = {
    label: string
    window: string
    pick: TimingPick
    color: string
    chartLabel: string
    requests: RequestTrace[] | null
    failed: boolean
    now: number
}

export function TimingCard({ label, window, pick, color, chartLabel, requests, failed, now }: TimingCardProps) {
    const stats = requests && timingStats(requests, now, pick)
    const loading = !stats && !failed
    const value = stats ? formatCount(Math.round(stats.average)) : loading ? <Skeleton className="h-11 w-20" /> : "—"
    return (
        <TrendCard label={label} window={window} value={value} unit="ms" busy={loading} series={stats?.series ?? emptySeries} axis={trendAxis(now)} color={color} chartLabel={chartLabel}>
            {stats ? (
                <>
                    <TrendStat label="p95" value={formatMs(stats.p95)} />
                    <TrendStat label="Max" value={formatMs(stats.max)} />
                </>
            ) : (
                <TrendStatsSkeleton failed={failed} />
            )}
        </TrendCard>
    )
}
