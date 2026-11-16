import type { ActorResidency } from "../types/Actors.js"
import type { ActorKill, KillReason } from "../types/Kills.js"
import type { ResidencyPeriod } from "../types/Residency.js"
import type { TimeWindow } from "../types/TimeRange.js"

export type InstanceEvent = { actorId: string; kind: ActorResidency | KillReason; atMs: number; kill?: ActorKill }

// A killed sandbox can't report its stop, so its instance turns unknown 35 s after the sandbox's
// last report. The kill already says why, so that change is left out.
const unknownAfterKillMs = 40_000

/** What happened to one instance in the window, newest first: its residency changes and its sandbox kills. */
export function instanceEvents(periods: ResidencyPeriod[], kills: ActorKill[], timeWindow: TimeWindow): InstanceEvent[] {
    const killed = killEvents(kills, timeWindow)
    const changes = residencyChanges(periods, timeWindow.fromMs).filter(change => !followsKill(change, killed))
    return [...killed, ...changes].sort((a, b) => b.atMs - a.atMs)
}

/** The window's sandbox kills, each under the instance it stopped, in the order given: a history lists them newest first. */
export function killEvents(kills: ActorKill[], { fromMs }: TimeWindow): InstanceEvent[] {
    return kills.filter(kill => kill.atMs >= fromMs).map(kill => ({ actorId: kill.actorId, kind: kill.reason, atMs: kill.atMs, kill }))
}

// A period that starts with the window began before it, so it isn't a change.
function residencyChanges(periods: ResidencyPeriod[], windowStartMs: number): InstanceEvent[] {
    return periods.filter(period => period.fromMs > windowStartMs).map(period => ({ actorId: period.actorId, kind: period.status, atMs: period.fromMs }))
}

function followsKill({ kind, atMs }: InstanceEvent, kills: InstanceEvent[]): boolean {
    return kind === "unknown" && kills.some(kill => kill.atMs <= atMs && atMs - kill.atMs <= unknownAfterKillMs)
}
