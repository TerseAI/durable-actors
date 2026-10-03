import { AsyncLocalStorage } from "node:async_hooks"

import { ActorDefinitionError } from "../errors.js"

type BackgroundTask = () => Promise<unknown>
const invocation = new AsyncLocalStorage<{
    actor: object
    active: boolean
    register: (task: BackgroundTask) => void
}>()

function waitUntil(actor: object, task: BackgroundTask): void {
    const scope = invocation.getStore()
    if (scope?.actor !== actor || !scope.active)
        throw new ActorDefinitionError("background tasks require an active actor invocation")
    if (typeof task !== "function") throw new ActorDefinitionError("waitUntil requires a deferred callback")
    scope.register(task)
}

class ActorBackgroundTasks {
    private sequence = 0
    private readonly pending = new Map<number, BackgroundTask>()

    async run<T>(
        actor: object,
        interleaved: boolean,
        operation: () => Promise<T>
    ): Promise<{ value: T; tasks: number[] }> {
        const tasks: number[] = []
        const scope = {
            actor,
            active: true,
            register: (task: BackgroundTask) => {
                if (interleaved) throw new ActorDefinitionError("background tasks require a serialized actor")
                if (this.pending.size >= 64) throw new ActorDefinitionError("actor background task limit reached")
                const id = ++this.sequence
                this.pending.set(id, task)
                tasks.push(id)
            }
        }
        try {
            return { value: await invocation.run(scope, operation), tasks }
        } catch (error) {
            this.discard(tasks)
            throw error
        } finally {
            scope.active = false
        }
    }

    take(id: unknown): BackgroundTask {
        const task = typeof id === "number" ? this.pending.get(id) : undefined
        if (task === undefined) throw new ActorDefinitionError("background task is no longer available")
        this.pending.delete(id as number)
        return task
    }

    discard(tasks: readonly number[]): void {
        for (const id of tasks) this.pending.delete(id)
    }

    clear(): void {
        this.pending.clear()
    }
}

export { ActorBackgroundTasks, waitUntil }
