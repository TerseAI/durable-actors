import { useParams } from "react-router"

import { useData } from "../../client/DatatContext.js"
import { useTimeWindow } from "../../client/TimeRangeContext.js"
import { formatPlural } from "../../format.js"
import { useRequestHistogram } from "../../hooks/useRequests.js"
import type { ActorTypeSummary } from "../../lib/actorTypes.js"
import { averageServerTime } from "../../lib/requestStats.js"
import { describeWindow } from "../../lib/timeRange.js"
import { LogsCard } from "../logs/logsCard.js"
import { RequestRateTrend } from "../overview/requestRateCard.js"
import { TimeRangeControl } from "../timeRange/timeRangeControl.js"
import { PageHeader } from "../ui/pageHeader.js"
import { Skeleton } from "../ui/skeleton.js"

import { InstancesCard, useInstancesTable } from "./instancesCard.js"
import { LiveDormantCard } from "./liveDormantCard.js"
import { TimingCard } from "./timingCard.js"

export function ActorTypePage() {
    const { actorName = "" } = useParams()
    return <ActorTypeContent key={actorName} actorName={actorName} />
}

function ActorTypeContent({ actorName }: { actorName: string }) {
    const timeWindow = useTimeWindow()
    const table = useInstancesTable(actorName, timeWindow.live)
    const { summary, methods, data, error } = table
    return (
        <section aria-labelledby="actor-type-heading" className="flex flex-col gap-5">
            <ActorTypeHeader actorName={actorName} summary={summary} loading={!summary && !error} />
            {summary && !summary.total && !methods ? (
                <p className="text-[13px] text-muted-foreground">No actor type named {actorName} is deployed.</p>
            ) : (
                <>
                    <ActorTypeMetrics actorName={actorName} summary={summary ?? null} failed={!data && Boolean(error)} />
                    <InstancesCard table={table} now={timeWindow.toMs} />
                    <LogsCard actorName={actorName} methods={methods} timeWindow={timeWindow} />
                </>
            )}
        </section>
    )
}

function ActorTypeMetrics({ actorName, summary, failed }: { actorName: string; summary: ActorTypeSummary | null; failed: boolean }) {
    const { baseUrl, events } = useData()
    const timeWindow = useTimeWindow()
    const { histogram, isError: isRequestsError } = useRequestHistogram(baseUrl, timeWindow, { actorName }, events)
    return (
        <div className="flex flex-col gap-4">
            <div className="grid gap-4 lg:grid-cols-3">
                <LiveDormantCard summary={summary} failed={failed} />
                <RequestRateTrend histogram={histogram} failed={isRequestsError} timeWindow={timeWindow} color="var(--foreground)" />
                <TimingCard
                    label="Avg Server Time"
                    pick={averageServerTime}
                    color="var(--chart-2)"
                    chartLabel={`Average server time, ${describeWindow(timeWindow)}`}
                    histogram={histogram}
                    failed={isRequestsError}
                    timeWindow={timeWindow}
                />
            </div>
        </div>
    )
}

function ActorTypeHeader({ actorName, summary, loading }: { actorName: string; summary: ActorTypeSummary | undefined; loading: boolean }) {
    return (
        <PageHeader
            titleId="actor-type-heading"
            title={actorName}
            back={{ to: "..", label: "Overview" }}
            meta={summary ? `Actor type · ${formatPlural(summary.total, "instance")}` : loading ? <Skeleton className="h-3.5 w-40" /> : "Actor type"}
            aside={<TimeRangeControl />}
        />
    )
}
