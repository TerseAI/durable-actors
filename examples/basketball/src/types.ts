export type ShotKind = "two" | "three" | "free"
export type CounterKind = "rebound" | "assist" | "steal" | "turnover"
export type EventKind = ShotKind | CounterKind

export type PracticeEvent = { id: string; kind: EventKind; made: number; createdAt: number }
export type ShotStats = { kind: ShotKind; made: number; attempts: number; percentage: number | null }

export type PracticeSummary = {
    points: number
    eventCount: number
    startedAt: number | null
    fieldGoalsMade: number
    fieldGoalsAttempted: number
    fieldGoalPercentage: number | null
    effectiveFieldGoalPercentage: number | null
    shots: ShotStats[]
    counters: { rebound: number; assist: number; steal: number; turnover: number }
    recent: PracticeEvent[]
    lastTen: PracticeEvent[]
}
