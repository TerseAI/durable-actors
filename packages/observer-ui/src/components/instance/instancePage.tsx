import { useParams } from "react-router"

import { useData } from "../../client/DatatContext.js"
import { useTimeWindow } from "../../client/TimeRangeContext.js"
import { formatAgo, formatClock, formatDuration } from "../../format.js"
import { useEventHistory } from "../../hooks/useEventHistory.js"
import { useInstancePage } from "../../hooks/useInstancePage.js"
import { useRequestHistogram } from "../../hooks/useRequests.js"
import { useResidency } from "../../hooks/useResidency.js"
import { useResources } from "../../hooks/useResources.js"
import { averageQueueWait, averageServerTime } from "../../lib/requestStats.js"
import { describeWindow } from "../../lib/timeRange.js"
import type { InstanceRow } from "../../types/Actors.js"
import type { ResidencyPeriod } from "../../types/Residency.js"
import type { TimeWindow } from "../../types/TimeRange.js"
import { InstanceStatus } from "../actorType/instanceStatus.js"
import { TimingCard } from "../actorType/timingCard.js"
import { LogsCard } from "../logs/logsCard.js"
import { RequestRateTrend } from "../overview/requestRateCard.js"
import { TimeRangeControl } from "../timeRange/timeRangeControl.js"
import { PageHeader } from "../ui/pageHeader.js"
import { Skeleton } from "../ui/skeleton.js"

import { CpuCard, MemoryCard, StorageCard } from "./resourceCards.js"

export function InstancePage() {
    const { actorName = "", actorId = "" } = useParams()
    const timeWindow = useTimeWindow()
    const { residency } = useResidency(actorName, timeWindow, actorId)
    const { data, error } = useInstancePage({ actorName, actorId, limit: 1 }, timeWindow.live)
    const instance = data?.instances[0] ?? null
    const livePeriods = residency && residency.periods.filter(period => period.actorId === actorId && period.status === "live")

    return (
        <section aria-labelledby="instance-heading" className="flex flex-col gap-5">
            <InstanceHeader actorName={actorName} actorId={actorId} instance={instance} livePeriods={livePeriods} timeWindow={timeWindow} loading={!data && !error} />
            {data && !instance ? (
                <p className="text-[13px] text-muted-foreground">
                    No {actorName} instance named {actorId} is deployed.
                </p>
            ) : (
                <>
                    <InstanceMetrics actorName={actorName} actorId={actorId} />
                    <LogsCard actorName={actorName} actorId={actorId} methods={data?.methods} timeWindow={timeWindow} />
                </>
            )}
        </section>
    )
}

function InstanceMetrics({ actorName, actorId }: { actorName: string; actorId: string }) {
    const { baseUrl, events } = useData()
    const timeWindow = useTimeWindow()
    const { histogram, isError: isHistogramError } = useRequestHistogram(baseUrl, timeWindow, { actorName, actorId }, events)
    const { resources, isError: isResourcesError } = useResources(actorName, actorId, timeWindow)
    const { history: killHistory } = useEventHistory(["kill"], timeWindow, actorName, actorId)
    const kills = (killHistory?.events ?? []).map(event => event.item).reverse()
    return (
        <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
            <RequestRateTrend histogram={histogram} failed={isHistogramError} timeWindow={timeWindow} color="var(--foreground)" />
            <TimingCard
                label="Avg Server Time"
                pick={averageServerTime}
                color="var(--chart-2)"
                chartLabel={`Average server time, ${describeWindow(timeWindow)}`}
                histogram={histogram}
                failed={isHistogramError}
                timeWindow={timeWindow}
            />
            <TimingCard
                label="Avg Queue Wait"
                pick={averageQueueWait}
                color="var(--chart-1)"
                chartLabel={`Average queue wait, ${describeWindow(timeWindow)}`}
                histogram={histogram}
                failed={isHistogramError}
                timeWindow={timeWindow}
            />
            <CpuCard resources={resources} failed={isResourcesError} timeWindow={timeWindow} />
            <MemoryCard resources={resources} kills={kills} failed={isResourcesError} timeWindow={timeWindow} />
            <StorageCard resources={resources} failed={isResourcesError} timeWindow={timeWindow} />
        </div>
    )
}

type InstanceHeaderProps = {
    actorName: string
    actorId: string
    instance: InstanceRow | null
    livePeriods: ResidencyPeriod[] | null
    timeWindow: TimeWindow
    loading: boolean
}

function InstanceHeader({ actorName, actorId, instance, livePeriods, timeWindow, loading }: InstanceHeaderProps) {
    const live = instance?.status === "live"
    return (
        <PageHeader
            titleId="instance-heading"
            title={actorId}
            back={{ to: "../..", relative: "path", label: actorName }}
            badge={instance && <StatusPill live={live} />}
            meta={
                loading ? (
                    <Skeleton className="h-3.5 w-64" />
                ) : (
                    <>
                        <span translate="no">{actorName}</span>
                        {instance && (
                            <>
                                <span aria-hidden>·</span>
                                <span>{residencyLabel(instance, livePeriods, timeWindow)}</span>
                            </>
                        )}
                    </>
                )
            }
            aside={<TimeRangeControl />}
        />
    )
}

function StatusPill({ live }: { live: boolean }) {
    return (
        <InstanceStatus
            live={live}
            className={live ? "rounded-full border border-success/25 bg-success/15 py-0.75 pr-2.25 pl-2 text-xs font-medium" : "rounded-full border px-2.25 py-0.75 text-xs font-medium"}
        >
            {live ? "Live" : "Dormant"}
        </InstanceStatus>
    )
}

function residencyLabel(instance: InstanceRow, livePeriods: ResidencyPeriod[] | null, { fromMs, toMs }: TimeWindow): string {
    if (instance.status !== "live") return instance.lastActiveAtMs ? `Last active ${formatAgo(instance.lastActiveAtMs, toMs)}` : "Dormant"
    const since = livePeriods?.at(-1)?.fromMs
    if (since === undefined) return "Live"
    return since <= fromMs ? "Live for the whole window" : `Live since ${formatClock(since)} (${formatDuration(toMs - since)})`
}
