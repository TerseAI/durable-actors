import { useData } from "../../client/DatatContext.js"
import { formatCount, formatDecimal, formatSignedPercent } from "../../format.js"
import { bucketCount, emptySeries, trendAxis, trendWindowMs } from "../../lib/trendWindow.js"
import type { ActorInventory } from "../../types/Actors.js"
import type { SocketMetrics } from "../../types/Overview.js"
import type { SocketSession } from "../../types/Sockets.js"
import { Delta } from "../ui/delta.js"
import { Skeleton } from "../ui/skeleton.js"
import { TrendCard, TrendStat, TrendStatsSkeleton } from "../ui/trendCard.js"

export function SocketsCard() {
    const { actorTypes, sessions, isError, isSocketsError } = useData()
    const now = Date.now()
    const failed = isError || isSocketsError
    const metrics = actorTypes && sessions && getMetrics(actorTypes, sessions, now)
    const loading = !metrics && !failed
    const value = metrics ? formatCount(metrics.open) : loading ? <Skeleton className="h-11 w-28" /> : "—"
    return (
        <TrendCard
            label="WebSocket Connections"
            window="open now"
            value={value}
            unit="open"
            busy={loading}
            series={metrics?.series ?? emptySeries}
            axis={trendAxis(now)}
            color="var(--chart-2)"
            chartLabel={metrics ? `Open WebSocket connections over the last hour, now ${formatCount(metrics.open)}` : "Open WebSocket connections over the last hour"}
        >
            {metrics ? <SocketStats {...metrics} /> : <TrendStatsSkeleton failed={failed} />}
        </TrendCard>
    )
}

function getMetrics(inventory: ActorInventory, sessions: SocketSession[], now: number): SocketMetrics {
    const instances = inventory.actors.flatMap(actor => actor.instances)
    const open = instances.reduce((sum, instance) => sum + instance.connections.length, 0)
    const live = instances.filter(instance => instance.status === "live").length
    const series = [...openSeries(sessions, liveConnections(inventory), now).slice(0, -1), open]
    const hourAgo = series[0] ?? 0
    return {
        open,
        change: hourAgo ? (open - hourAgo) / hourAgo : 0,
        perLiveActor: live ? open / live : 0,
        series
    }
}

function liveConnections(inventory: ActorInventory): Set<string> {
    return new Set(inventory.actors.flatMap(actor => actor.instances.flatMap(instance => instance.connections.map(connection => connectionKey(actor.actorName, instance.actorId, connection.id)))))
}

function openSeries(sessions: SocketSession[], live: Set<string>, now: number): number[] {
    const step = trendWindowMs / (bucketCount - 1)
    const start = now - trendWindowMs
    return Array.from({ length: bucketCount }, (_, index) => sessions.filter(session => isOpenAt(session, live, start + index * step)).length)
}

// Sessions with no disconnect that the inventory no longer reports are treated as closed at their last activity.
function isOpenAt(session: SocketSession, live: Set<string>, at: number): boolean {
    const opened = session.openedAtMs ?? session.lastSeenMs
    if (opened === null || opened > at) return false
    const isLive = live.has(connectionKey(session.actorName, session.actorId, session.connectionId))
    const closed = session.closedAtMs ?? (isLive ? Infinity : (session.lastSeenMs ?? opened))
    return at < closed
}

function connectionKey(actorName: string, actorId: string, connectionId: string): string {
    return JSON.stringify([actorName, actorId, connectionId])
}

function SocketStats({ change, perLiveActor }: SocketMetrics) {
    return (
        <>
            <span className="flex items-center gap-2">
                <Delta value={change}>{formatSignedPercent(change)}</Delta>
                vs 1&nbsp;h ago
            </span>
            <TrendStat label="Avg" value={formatDecimal(perLiveActor)} suffix="per live actor" />
        </>
    )
}
