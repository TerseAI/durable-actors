import { Actor, Ephemeral, Persisted } from "durable-actors"
import { existsSync } from "node:fs"

export class BackgroundProbe extends Actor<null, null, { count: number }> {
    @Persisted count = 0
    @Ephemeral instance = crypto.randomUUID()

    async start(gate: string): Promise<string> {
        this.count++
        this.waitUntil(async () => {
            const deadline = Date.now() + 10000
            while (!existsSync(gate)) {
                if (Date.now() >= deadline) throw new Error("test did not release background task")
                await new Promise(resolve => setTimeout(resolve, 10))
            }
            this.db.exec("CREATE TABLE IF NOT EXISTS background(value INTEGER)")
            this.db.exec("INSERT INTO background VALUES (?)", ++this.count)
            this.broadcast({ count: this.count })
        })
        return this.instance
    }

    async siblings(): Promise<void> {
        this.waitUntil(async () => {
            this.count = 99
            throw new Error("background failed")
        })
        this.waitUntil(async () => {
            this.count++
        })
    }

    async read(): Promise<{ count: number; instance: string }> {
        return { count: this.count, instance: this.instance }
    }
}
