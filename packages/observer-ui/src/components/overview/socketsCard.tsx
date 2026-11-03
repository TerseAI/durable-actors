import { formatCount, formatDecimal, formatSignedPercent } from "../../format.js"
import type { SocketMetrics } from "../../types/Overview.js"
import { Delta } from "../ui/delta.js"

import { TrendCard, TrendStat } from "./trendCard.js"

export function SocketsCard({ open, change, perLiveActor, series, axis }: SocketMetrics & { axis: string[] }) {
    return (
        <TrendCard
            label="WebSocket Connections"
            window="open now"
            value={formatCount(open)}
            unit="open"
            series={series}
            axis={axis}
            color="var(--chart-2)"
            chartLabel={`Open WebSocket connections over the last hour, now ${formatCount(open)}`}
        >
            <span className="flex items-center gap-2">
                <Delta value={change}>{formatSignedPercent(change)}</Delta>
                vs 1&nbsp;h ago
            </span>
            <TrendStat label="Avg" value={formatDecimal(perLiveActor)} suffix="per live actor" />
        </TrendCard>
    )
}
