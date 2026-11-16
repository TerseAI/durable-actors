import type { ActorResidency } from "../types/Actors.js"
import type { ActorKill, KillReason } from "../types/Kills.js"
import type { ResidencyPeriod } from "../types/Residency.js"
import type { TimeWindow } from "../types/TimeRange.js"

export type InstanceEvent = { kind: ActorResidency | KillReason; atMs: number; kill?: ActorKill }

// A killed sandbox can't report its stop, so its instance turns unknown 35 s after the sandbox's
// last report. The kill already says why, so that change is left out.
const unknownAfterKillMs = 40_000

/** What happened to one instance in the window, newest first: its residency changes and its sandbox kills. */
export function instanceEvents(periods: ResidencyPeriod[], kills: ActorKill[], { fromMs }: TimeWindow): InstanceEvent[] {
    const killed = kills.filter(kill => kill.atMs >= fromMs)
    const changes = residencyChanges(periods, fromMs).filter(change => !followsKill(change, killed))
    return [...killed.map(kill => ({ kind: kill.reason, atMs: kill.atMs, kill })), ...changes].sort((a, b) => b.atMs - a.atMs)
}

// A period that starts with the window began before it, so it isn't a change.
function residencyChanges(periods: ResidencyPeriod[], windowStartMs: number): InstanceEvent[] {
    return periods.filter(period => period.fromMs > windowStartMs).map(period => ({ kind: period.status, atMs: period.fromMs }))
}

function followsKill({ kind, atMs }: InstanceEvent, kills: ActorKill[]): boolean {
    return kind === "unknown" && kills.some(kill => kill.atMs <= atMs && atMs - kill.atMs <= unknownAfterKillMs)
}
