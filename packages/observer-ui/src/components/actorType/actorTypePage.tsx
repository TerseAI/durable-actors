import { Link, useParams } from "react-router"

import { ArrowLeft } from "lucide-react"

import { useData } from "../../client/DatatContext.js"
import { useTimeWindow } from "../../client/TimeRangeContext.js"
import { formatPlural } from "../../format.js"
import { useResidency } from "../../hooks/useResidency.js"
import { summarizeActorType } from "../../lib/actorTypes.js"
import { queueTime, serviceTime } from "../../lib/requestStats.js"
import { describeWindow } from "../../lib/timeRange.js"
import type { ActorType } from "../../types/Actors.js"
import { TimeRangeControl } from "../timeRange/timeRangeControl.js"
import { Skeleton } from "../ui/skeleton.js"

import { InstancesCard } from "./instancesCard.js"
import { LiveDormantCard } from "./liveDormantCard.js"
import { ResidencyCard } from "./residencyCard.js"
import { TimingCard } from "./timingCard.js"

export function ActorTypePage() {
    const { actorName = "" } = useParams()
    const { actorTypes, isError, requests, isRequestsError } = useData()
    const timeWindow = useTimeWindow()
    const { residency, isError: isResidencyError } = useResidency(actorName, timeWindow)
    const actor = actorTypes?.actors.find(type => type.actorName === actorName) ?? null
    const typeRequests = requests?.filter(trace => trace.actorName === actorName) ?? null

    return (
        <section aria-labelledby="actor-type-heading" className="flex flex-col gap-5">
            <ActorTypeHeader actorName={actorName} actor={actor} loading={!actorTypes && !isError} />
            {actorTypes && !actor ? (
                <p className="text-[13px] text-muted-foreground">No actor type named {actorName} is deployed.</p>
            ) : (
                <div className="flex flex-col gap-4">
                    <div className="grid gap-4 lg:grid-cols-3">
                        <LiveDormantCard summary={actor && summarizeActorType(actor)} failed={isError} />
                        <TimingCard
                            label="Avg Queue Time"
                            window={describeWindow(timeWindow)}
                            pick={queueTime}
                            color="var(--chart-1)"
                            chartLabel={`Average queue time, ${describeWindow(timeWindow)}`}
                            requests={typeRequests}
                            failed={isRequestsError}
                            timeWindow={timeWindow}
                        />
                        <TimingCard
                            label="Avg Request Latency"
                            window="excl. queue"
                            pick={serviceTime}
                            color="var(--chart-2)"
                            chartLabel={`Average request latency, ${describeWindow(timeWindow)}`}
                            requests={typeRequests}
                            failed={isRequestsError}
                            timeWindow={timeWindow}
                        />
                    </div>
                    <InstancesCard actor={actor} requests={typeRequests} failed={isError} now={timeWindow.toMs} />
                    <ResidencyCard actor={actor} history={residency} failed={isError || isResidencyError} timeWindow={timeWindow} />
                </div>
            )}
        </section>
    )
}

function ActorTypeHeader({ actorName, actor, loading }: { actorName: string; actor: ActorType | null; loading: boolean }) {
    return (
        <header className="flex flex-wrap items-end justify-between gap-4">
            <div>
                <Link to=".." className="inline-flex items-center gap-1.5 text-[13px] font-medium text-muted-foreground hover:text-foreground">
                    <ArrowLeft className="size-3.5" aria-hidden />
                    Overview
                </Link>
                <h1 id="actor-type-heading" translate="no" className="mt-2.5 font-mono text-[26px] font-medium tracking-tight">
                    {actorName}
                </h1>
                <div className="mt-1 text-[13px] text-muted-foreground tabular-nums">
                    {actor ? `Actor type · ${formatPlural(actor.instances.length, "instance")}` : loading ? <Skeleton className="h-3.5 w-40" /> : "Actor type"}
                </div>
            </div>
            <TimeRangeControl />
        </header>
    )
}
