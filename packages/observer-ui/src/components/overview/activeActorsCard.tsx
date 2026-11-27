import { formatCount } from "../../format.js"
import type { ActorResidencyMetrics } from "../../types/Overview.js"
import { ResidencyBar, ResidencyBarSkeleton, ResidencyLegend } from "../ui/residencyBar.js"
import { Skeleton } from "../ui/skeleton.js"
import { StatCard } from "../ui/statCard.js"

export function ActiveActorsCard({ metrics, isError }: { metrics: ActorResidencyMetrics | undefined; isError: boolean }) {
    const loading = !metrics && !isError
    const value = metrics ? formatCount(metrics.live) : loading ? <Skeleton className="h-11 w-28" /> : "—"
    const unit = loading ? <Skeleton className="h-3.5 w-7" /> : "live"
    return (
        <StatCard label="Active Actors" value={value} unit={unit} busy={loading}>
            {metrics ? <Residency {...metrics} /> : <ResidencySkeleton failed={isError} />}
        </StatCard>
    )
}

function Residency({ live, dormant }: ActorResidencyMetrics) {
    const total = live + dormant
    const liveShare = total ? live / total : 0
    return (
        <>
            <ResidencyBar liveShare={liveShare} label={`${formatCount(live)} live actors, ${formatCount(dormant)} dormant actors`} pulse className="mt-auto h-10 rounded-sm" />
            <ResidencyLegend live={live} dormant={dormant} />
        </>
    )
}

function ResidencySkeleton({ failed }: { failed: boolean }) {
    if (failed) return <p className="mt-2 text-[13px] text-muted-foreground">Couldn't reach the observer API.</p>
    return <ResidencyBarSkeleton />
}
