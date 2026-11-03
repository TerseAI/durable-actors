import { ActiveActorsCard } from "./activeActorsCard.js"
import { RequestRateCard } from "./requestRateCard.js"
import { SocketsCard } from "./socketsCard.js"
import { useData } from "../../client/DatatContext.js"
import type { ActorResidencyMetrics, OverviewMetrics } from "../../types/Overview.js"


import type { ActorInventory } from "../../types/Actors.js"

export function Overview({ metrics }: { metrics: OverviewMetrics }) {
    const { actorTypes } = useData()
    
    return (
        <section aria-labelledby="overview-heading" className="flex flex-col gap-5">
            <header className="flex flex-wrap items-baseline justify-between gap-2">
                <h1 id="overview-heading" className="text-2xl font-bold tracking-tight">
                    Overview
                </h1>
                <span className="text-xs text-muted-foreground tabular-nums">{metrics.updatedLabel}</span>
            </header>
            <div className="grid gap-4 lg:grid-cols-[1.45fr_1fr_1fr]">
                <ActiveActorsCard {...getMetrics(actorTypes)} />
                <RequestRateCard {...metrics.requests} axis={metrics.axis} />
                <SocketsCard {...metrics.sockets} axis={metrics.axis} />
            </div>
        </section>
    )
}


function getMetrics(actorTypes: ActorInventory): ActorResidencyMetrics {
    const actorInstances = actorTypes.actors.flatMap((actor) => actor.instances)
    const live = actorInstances.filter((instance) => instance.status === "live").length
    const dormant = actorInstances.filter((instance) => instance.status === "dormant").length
    return {
        live: live,
        dormant: dormant,
        liveDelta: live - dormant
    }
}