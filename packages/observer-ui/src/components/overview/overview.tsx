import { ActiveActorsCard } from "./activeActorsCard.js"
import { RequestRateCard } from "./requestRateCard.js"
import { SocketsCard } from "./socketsCard.js"
import type { OverviewMetrics } from "../../types/Overview.js"

export function Overview({ metrics }: { metrics: OverviewMetrics }) {
    return (
        <section aria-labelledby="overview-heading" className="flex flex-col gap-5">
            <header className="flex flex-wrap items-baseline justify-between gap-2">
                <h1 id="overview-heading" className="text-2xl font-bold tracking-tight">
                    Overview
                </h1>
            </header>
            <div className="grid gap-4 lg:grid-cols-3">
                <ActiveActorsCard />
                <RequestRateCard />
                <SocketsCard {...metrics.sockets} axis={metrics.axis} />
            </div>
        </section>
    )
}
