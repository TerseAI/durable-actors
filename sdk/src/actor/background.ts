import { AsyncLocalStorage } from "node:async_hooks"

import { ActorDefinitionError } from "../errors.js"
import { cloneJson } from "../json.js"
import type { JsonValue } from "../json.js"

type ExternalTask = () => Promise<{ method: string; args: JsonValue[] }>

export type TaskOutcome<Input, Output> =
    { input: Input; ok: true; value: Output } | { input: Input; ok: false; error: string }

type BackgroundTask = () => Promise<unknown>
const invocation = new AsyncLocalStorage<{
    actor: object
    active: boolean
    register: (task: BackgroundTask, external?: boolean) => void
}>()

function waitUntil(actor: object, task: BackgroundTask): void {
    const scope = invocation.getStore()
    if (scope?.actor !== actor || !scope.active)
        throw new ActorDefinitionError("background tasks require an active actor invocation")
    if (typeof task !== "function") throw new ActorDefinitionError("waitUntil requires a deferred callback")
    scope.register(task)
}

function runTask<Input, Output>(
    actor: object,
    task: (input: Input) => Promise<Output>,
    input: Input,
    completion: string
): void {
    const scope = invocation.getStore()
    if (scope?.actor !== actor || !scope.active)
        throw new ActorDefinitionError("external tasks require an active actor invocation")
    const constructor = actor.constructor
    if (!Object.values(Object.getOwnPropertyDescriptors(constructor)).some(descriptor => descriptor.value === task))
        throw new ActorDefinitionError("runTask requires a static method on the actor class")
    const method = Object.getOwnPropertyDescriptor(Object.getPrototypeOf(actor), completion)?.value
    if (
        typeof method !== "function" ||
        completion.startsWith("__") ||
        ["onConnect", "onMessage", "onDisconnect", "onAlarm"].includes(completion)
    )
        throw new ActorDefinitionError("runTask requires a public completion method")
    const value = cloneJson(input, "task input")
    scope.register(async () => {
        let outcome: JsonValue
        try {
            const result = await Reflect.apply(task, undefined, [cloneJson(value, "task input")])
            outcome = { input: value, ok: true, value: result === undefined ? null : cloneJson(result, "task result") }
        } catch (error) {
            outcome = { input: value, ok: false, error: error instanceof Error ? error.message : String(error) }
        }
        return { method: completion, args: [outcome] }
    }, true)
}

class ActorBackgroundTasks {
    private sequence = 0
    private readonly pending = new Map<number, BackgroundTask>()

    async run<T>(
        actor: object,
        interleaved: boolean,
        operation: () => Promise<T>
    ): Promise<{ value: T; tasks: number[]; externalTasks: number[] }> {
        const tasks: number[] = []
        const externalTasks: number[] = []
        const scope = {
            actor,
            active: true,
            register: (task: BackgroundTask, external = false) => {
                if (interleaved) throw new ActorDefinitionError("background tasks require a serialized actor")
                if (this.pending.size >= 64) throw new ActorDefinitionError("actor background task limit reached")
                const id = ++this.sequence
                this.pending.set(id, task)
                ;(external ? externalTasks : tasks).push(id)
            }
        }
        try {
            return { value: await invocation.run(scope, operation), tasks, externalTasks }
        } catch (error) {
            this.discard([...tasks, ...externalTasks])
            throw error
        } finally {
            scope.active = false
        }
    }

    get hasPending(): boolean {
        return this.pending.size > 0
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

export { ActorBackgroundTasks, runTask, waitUntil }
export type { ExternalTask }
