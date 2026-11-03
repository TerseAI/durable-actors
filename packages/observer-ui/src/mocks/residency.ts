import { trendWindowMs } from "../lib/trendWindow.js"
import type { ActorInstance } from "../types/Actors.js"
import type { ResidencyHistory, ResidencyPeriod } from "../types/Residency.js"

const minute = 60_000

export function mockResidencyHistory(instances: ActorInstance[], now: number): ResidencyHistory {
    return { truncated: false, periods: instances.flatMap(instance => mockPeriods(instance, now)) }
}

function mockPeriods({ actorId, status }: ActorInstance, now: number): ResidencyPeriod[] {
    const random = seeded(actorId)
    const start = now - trendWindowMs
    const periods: ResidencyPeriod[] = []
    let cursor = start + Math.floor(random() * 20) * minute
    while (cursor < now) {
        const length = (4 + Math.floor(random() * 22)) * minute
        periods.push({ actorId, status: "live", fromMs: cursor, toMs: Math.min(cursor + length, now) })
        cursor += length + (3 + Math.floor(random() * 18)) * minute
    }
    const last = periods.at(-1)
    if (status === "live" && last) last.toMs = now
    if (status !== "live" && last?.toMs === now) last.toMs = now - 4 * minute
    return periods.filter(period => period.toMs > period.fromMs)
}

function seeded(text: string): () => number {
    let state = [...text].reduce((hash, char) => (hash * 31 + char.charCodeAt(0)) >>> 0, 7)
    return () => {
        state = (state * 1_664_525 + 1_013_904_223) >>> 0
        return state / 2 ** 32
    }
}
