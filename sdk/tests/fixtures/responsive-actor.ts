import { Actor, Persisted } from "durable-actors"
import { existsSync, writeFileSync } from "node:fs"

type Work = { gate: string; generation: number; fail: boolean }
type Outcome = { input: Work; ok: boolean; value?: string; error?: string }

export class ResponsiveSession extends Actor<null, null, { state: string }> {
    @Persisted generation = 0
    @Persisted state = "idle"
    @Persisted heartbeats = 0

    async start(gate: string, fail: boolean, pair = false): Promise<number> {
        this.generation++
        this.state = "provisioning"
        const work = { gate, generation: this.generation, fail }
        this.runTask(ResponsiveSession.provision, work, "complete")
        if (pair) this.runTask(ResponsiveSession.provision, { ...work, gate: gate + ".sibling" }, "complete")
        work.generation = -1
        return this.generation
    }

    static async provision(work: Work): Promise<string> {
        writeFileSync(work.gate + ".entered", "")
        const deadline = Date.now() + 10000
        while (!existsSync(work.gate)) {
            if (Date.now() >= deadline) throw new Error("gate was not released")
            await new Promise(resolve => setTimeout(resolve, 10))
        }
        if (work.fail) throw new Error("provisioning failed")
        work.generation = -2
        return "ready"
    }

    async complete(outcome: Outcome): Promise<void> {
        if (outcome.input.generation === this.generation) this.state = outcome.ok ? outcome.value! : "failed"
        this.broadcast({ state: this.state })
    }

    async cancel(): Promise<void> {
        this.generation++
        this.state = "canceled"
    }
    async heartbeat(): Promise<number> {
        return ++this.heartbeats
    }
    async finish_terminal(): Promise<void> {
        this.state = "terminal_done"
    }
    async fail(): Promise<void> {
        this.state = "corrupt"
        throw new Error("rollback")
    }
    async read(): Promise<{ state: string; heartbeats: number }> {
        return { state: this.state, heartbeats: this.heartbeats }
    }
}
