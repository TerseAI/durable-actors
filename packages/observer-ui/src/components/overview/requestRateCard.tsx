import { formatCount } from "../../format.js"
import type { RequestRateMetrics } from "../../types/Overview.js"

import { TrendCard, TrendStat } from "./trendCard.js"

export function RequestRateCard({ current, average, peak, peakAt, series, axis }: RequestRateMetrics & { axis: string[] }) {
    return (
        <TrendCard
            label="Requests / Second"
            window={"last 1\u00a0h"}
            value={formatCount(current)}
            unit="req/s"
            series={series}
            axis={axis}
            color="var(--chart-1)"
            chartLabel={`Requests per second over the last hour, peaking at ${formatCount(peak)}`}
        >
            <TrendStat label="Avg" value={formatCount(average)} />
            <TrendStat label="Peak" value={formatCount(peak)} suffix={`at ${peakAt}`} />
        </TrendCard>
    )
}
