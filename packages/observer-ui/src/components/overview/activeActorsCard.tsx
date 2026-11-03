import { formatCount, formatPercent, formatSignedCount } from "../../format.js"
import type { ActorResidencyMetrics } from "../../types/Overview.js"
import { Delta } from "../ui/delta.js"
import { StatCard } from "../ui/statCard.js"

const liveTicks = "bg-[repeating-linear-gradient(90deg,var(--success)_0_3px,transparent_3px_6px)]"
const dormantTicks = "bg-[repeating-linear-gradient(90deg,var(--border)_0_3px,transparent_3px_6px)]"

export function ActiveActorsCard({ live, dormant, liveDelta }: ActorResidencyMetrics) {
    const total = live + dormant
    const liveShare = total ? live / total : 0
    return (
        <StatCard label="Active Actors" aside={<ViewAllLink />} value={formatCount(live)} unit="live">
            <p className="mt-2 flex items-center gap-2 text-[13px] text-muted-foreground tabular-nums">
                <Delta value={liveDelta}>{formatSignedCount(liveDelta)}</Delta>
                since 1&nbsp;h ago · {formatCount(total)} {total === 1 ? "actor" : "actors"} total
            </p>
            <ResidencyBar live={live} dormant={dormant} liveShare={liveShare} />
            <div className="mt-3 flex justify-between gap-4 text-[13px] tabular-nums">
                <LegendItem swatch="bg-success" label="Live" count={live} share={liveShare} />
                <LegendItem swatch="bg-border" label="Dormant" count={dormant} share={total ? 1 - liveShare : 0} />
            </div>
        </StatCard>
    )
}

function ViewAllLink() {
    return (
        <a href="#actors" className="shrink-0 text-xs font-medium text-primary hover:underline">
            View All
        </a>
    )
}

function ResidencyBar({ live, dormant, liveShare }: { live: number; dormant: number; liveShare: number }) {
    return (
        <div role="img" aria-label={`${formatCount(live)} live actors, ${formatCount(dormant)} dormant actors`} className="mt-auto flex h-10 overflow-hidden rounded-sm">
            <div className={`relative ${liveTicks}`} style={{ width: `${liveShare * 100}%` }}>
                <span className="absolute inset-y-0 right-0.75 w-0.75 animate-pulse bg-success motion-reduce:animate-none" />
            </div>
            <div className={`flex-1 ${dormantTicks}`} />
        </div>
    )
}

function LegendItem({ swatch, label, count, share }: { swatch: string; label: string; count: number; share: number }) {
    return (
        <span className="flex items-center gap-1.75 text-muted-foreground">
            <span className={`size-2 rounded-xs ${swatch}`} />
            {label} <b className="font-bold text-foreground">{formatCount(count)}</b> · {formatPercent(share)}
        </span>
    )
}
