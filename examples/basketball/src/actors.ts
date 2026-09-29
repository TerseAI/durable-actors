import { Actor } from "durable-actors"

import type { EventKind, PracticeEvent, PracticeSummary, ShotKind, ShotStats } from "./types.js"

const shotKinds: ShotKind[] = ["two", "three", "free"]
const eventKinds: EventKind[] = [...shotKinds, "rebound", "assist", "steal", "turnover"]

export class Practice extends Actor {
    async summary(): Promise<PracticeSummary> {
        this.#ensureTable()
        return this.#summary()
    }

    async record(id: string, kind: EventKind, made: boolean): Promise<PracticeSummary> {
        if (!/^[a-zA-Z0-9-]{1,128}$/.test(id) || !eventKinds.includes(kind)) throw new Error("Invalid practice event")
        if (!shotKinds.includes(kind as ShotKind) && made) throw new Error("Only shots can be made")
        this.#ensureTable()
        this.db.exec("INSERT INTO events (id, kind, made, created_at) VALUES (?, ?, ?, ?) ON CONFLICT(id) DO NOTHING", id, kind, Number(made), Date.now())
        return this.#summary()
    }

    async undo(id: string): Promise<PracticeSummary> {
        this.#ensureTable()
        this.db.exec("DELETE FROM events WHERE id = ? AND sequence = (SELECT MAX(sequence) FROM events)", id)
        return this.#summary()
    }

    #summary(): PracticeSummary {
        const shots = this.#shots()
        const [two, three, free] = shots
        const fieldGoalsMade = two!.made + three!.made
        const fieldGoalsAttempted = two!.attempts + three!.attempts
        const totals = this.db.exec<{ eventCount: number; startedAt: number | null }>("SELECT COUNT(*) AS eventCount, MIN(created_at) AS startedAt FROM events")[0]!
        return {
            ...totals,
            points: two!.made * 2 + three!.made * 3 + free!.made,
            fieldGoalsMade,
            fieldGoalsAttempted,
            fieldGoalPercentage: percentage(fieldGoalsMade, fieldGoalsAttempted),
            effectiveFieldGoalPercentage: percentage(fieldGoalsMade + three!.made * 0.5, fieldGoalsAttempted),
            shots,
            counters: this.#counters(),
            recent: this.db.exec<PracticeEvent>("SELECT id, kind, made, created_at AS createdAt FROM events ORDER BY sequence DESC LIMIT 12"),
            lastTen: this.db.exec<PracticeEvent>("SELECT id, kind, made, created_at AS createdAt FROM events WHERE kind IN ('two', 'three', 'free') ORDER BY sequence DESC LIMIT 10").reverse()
        }
    }

    #shots(): ShotStats[] {
        const rows = this.db.exec<{ kind: ShotKind; made: number; attempts: number }>(
            "SELECT kind, SUM(made) AS made, COUNT(*) AS attempts FROM events WHERE kind IN ('two', 'three', 'free') GROUP BY kind"
        )
        return shotKinds.map(kind => {
            const { made = 0, attempts = 0 } = rows.find(row => row.kind === kind) ?? {}
            return { kind, made, attempts, percentage: percentage(made, attempts) }
        })
    }

    #counters(): PracticeSummary["counters"] {
        const rows = this.db.exec<{ kind: string; count: number }>("SELECT kind, COUNT(*) AS count FROM events GROUP BY kind")
        const count = (kind: string) => rows.find(row => row.kind === kind)?.count ?? 0
        return { rebound: count("rebound"), assist: count("assist"), steal: count("steal"), turnover: count("turnover") }
    }

    #ensureTable(): void {
        this.db.exec(
            "CREATE TABLE IF NOT EXISTS events (sequence INTEGER PRIMARY KEY, id TEXT NOT NULL UNIQUE, kind TEXT NOT NULL, made INTEGER NOT NULL CHECK(made IN (0, 1)), created_at INTEGER NOT NULL)"
        )
    }
}

function percentage(made: number, attempted: number): number | null {
    return attempted === 0 ? null : Math.round((made / attempted) * 1000) / 10
}
