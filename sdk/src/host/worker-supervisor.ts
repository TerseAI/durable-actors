import { Worker } from "node:worker_threads"

import type { ActorIdentity } from "../actor/identity.js"
import { actorKey } from "../actor/identity.js"
import type { ActorSchema } from "../actor/schema.js"
import type { SocketEffect } from "../actor/socketProtocol.js"
import { errorMessage } from "../errors.js"

import { failedReply } from "./protocol.js"
import type {
    ActorExecutorCommand,
    ActorExecutorReply,
    ActorWorkerData,
    ActorWorkerMessage,
    ActorWorkerRequest,
    EvictCommand,
    InvokeCommand,
    WebSocketEventCommand
} from "./protocol.js"
import type {
    ActorWorkerFactory,
    ActorWorkerHandle,
    ActorWorkerSupervisorOptions,
    ResidentActorWorkerOptions,
    SocketPublisher,
    SocketSource
} from "./types.js"

const DEFAULT_ACTOR_IDLE_TIMEOUT_MS = 60_000

const MAX_RESIDENT_ACTORS = 32

class ActorWorkerSupervisor {
    private readonly actorEntrypointUrl: string
    private readonly actorSchemas: readonly ActorSchema[] | undefined
    private readonly actorIdleTimeoutMs: number
    private readonly createWorker: ActorWorkerFactory
    private lastResidency = ""
    private readonly residencyListeners = new Set<() => void>()
    private readonly actors = new Map<string, ResidentActorWorker>()
    private speculativeWorker: ActorWorkerHandle | undefined
    private speculativeTimer: NodeJS.Timeout | undefined
    private closed = false
    private actorTypes: readonly string[] | undefined

    constructor(options: ActorWorkerSupervisorOptions) {
        this.actorEntrypointUrl = options.actorEntrypointUrl
        this.actorSchemas = options.actorSchemas
        this.actorIdleTimeoutMs = options.actorIdleTimeoutMs ?? DEFAULT_ACTOR_IDLE_TIMEOUT_MS
        this.createWorker = options.createWorker ?? (data => new ActorWorker(data))
        if (!Number.isInteger(this.actorIdleTimeoutMs) || this.actorIdleTimeoutMs <= 0) {
            throw new Error("actor idle timeout must be a positive integer")
        }
        this.preload()
    }

    async ready(): Promise<readonly string[]> {
        if (this.closed) throw new Error("actor supervisor is closed")
        if (this.actorTypes !== undefined) return this.actorTypes
        const worker = this.speculativeWorker ?? this.preload()
        if (this.speculativeTimer !== undefined) clearTimeout(this.speculativeTimer)
        this.actorTypes = await worker.ready()
        if (this.speculativeWorker === worker) {
            this.speculativeTimer = setTimeout(() => this.discardPreload(worker), this.actorIdleTimeoutMs)
            this.speculativeTimer.unref()
        }
        return this.actorTypes
    }

    async handle(
        command: ActorExecutorCommand,
        publish?: SocketPublisher,
        connections?: SocketSource
    ): Promise<ActorExecutorReply> {
        if (this.closed) return failedReply("actor_worker_terminated", "actor supervisor is closed")
        switch (command.type) {
            case "invoke":
            case "websocket_event":
                try {
                    if (this.actorTypes === undefined) await this.ready()
                    return await this.execute(command, publish, connections)
                } catch (error) {
                    return failedReply("actor_worker_failed", errorMessage(error))
                }
            case "evict":
                return this.evict(command)
            default:
                throw command satisfies never
        }
    }

    onResidencyChange(listener: () => void): () => void {
        this.residencyListeners.add(listener)
        return () => {
            this.residencyListeners.delete(listener)
        }
    }

    private notifyResidencyChange(): void {
        const current = JSON.stringify(this.residentActors())
        if (current === this.lastResidency) return
        this.lastResidency = current
        for (const listener of this.residencyListeners) listener()
    }

    residentActors(): readonly ActorIdentity[] {
        return [...this.actors.entries()]
            .filter(([, actor]) => actor.isResident())
            .map(([key]) => {
                const [namespace_id, actor_type, actor_id] = key.split("\u001f") as [string, string, string]
                return { namespace_id, actor_type, actor_id }
            })
    }

    close(): void {
        this.closed = true
        this.takeSpeculativeWorker()?.terminate("actor supervisor is closed")
        for (const actor of this.actors.values()) actor.terminate("actor supervisor is closed")
        this.actors.clear()
    }

    private preload(): ActorWorkerHandle {
        const worker = this.createWorker({ moduleUrl: this.actorEntrypointUrl, schemas: this.actorSchemas })
        this.speculativeWorker = worker
        this.speculativeTimer = setTimeout(() => this.discardPreload(worker), this.actorIdleTimeoutMs)
        this.speculativeTimer.unref()
        void worker.ready().catch(() => this.discardPreload(worker))
        return worker
    }

    private discardPreload(worker: ActorWorkerHandle): void {
        if (this.speculativeWorker !== worker) return
        this.takeSpeculativeWorker()?.terminate("unused actor preload expired or failed")
    }

    private execute(
        command: InvokeCommand | WebSocketEventCommand,
        publish?: SocketPublisher,
        connections?: SocketSource
    ): Promise<ActorExecutorReply> {
        if (!this.actorTypes?.includes(command.actor.actor_type)) {
            return Promise.resolve(
                failedReply(
                    "actor_type_not_found",
                    `actor type ${command.actor.actor_type} is not loaded in this customer process`
                )
            )
        }
        const key = actorKey(command.actor)
        let actor = this.actors.get(key)
        if (actor === undefined) {
            if (command.resident_only) return Promise.resolve({ type: "state_required" })
            if (!this.makeRoomForActor()) {
                return Promise.resolve(
                    failedReply(
                        "resource_exhausted",
                        `actor host already has ${MAX_RESIDENT_ACTORS} active actor Workers`
                    )
                )
            }
            actor = new ResidentActorWorker({
                moduleUrl: this.actorEntrypointUrl,
                schemas: this.actorSchemas,
                idleTimeoutMs: this.actorIdleTimeoutMs,
                worker: this.takeSpeculativeWorker(),
                createWorker: this.createWorker,
                onIdle: candidate => this.removeIfCurrent(key, candidate),
                onResidencyChange: () => this.notifyResidencyChange()
            })
            this.actors.set(key, actor)
        }
        return actor.execute(command, publish, connections)
    }

    private evict(command: EvictCommand): ActorExecutorReply {
        const key = actorKey(command.actor)
        const actor = this.actors.get(key)
        if (actor !== undefined) {
            this.actors.delete(key)
            actor.terminate("resident actor was evicted by the Rust host")
        }
        return { type: "evicted" }
    }

    private makeRoomForActor(): boolean {
        if (this.actors.size < MAX_RESIDENT_ACTORS) return true
        const idle = [...this.actors.entries()]
            .filter(([, actor]) => actor.isIdle())
            .sort((left, right) => left[1].lastCompletedAt - right[1].lastCompletedAt)[0]
        if (idle === undefined) return false
        this.actors.delete(idle[0])
        idle[1].terminate("resident actor was evicted by the LRU capacity limit")
        return true
    }

    private removeIfCurrent(key: string, actor: ResidentActorWorker): void {
        if (this.actors.get(key) !== actor || !actor.isIdle()) return
        this.actors.delete(key)
        actor.terminate(`resident actor was idle for ${this.actorIdleTimeoutMs}ms`)
    }

    private takeSpeculativeWorker(): ActorWorkerHandle | undefined {
        if (this.speculativeTimer !== undefined) clearTimeout(this.speculativeTimer)
        this.speculativeTimer = undefined
        const worker = this.speculativeWorker
        this.speculativeWorker = undefined
        return worker
    }
}

class ResidentActorWorker {
    readonly moduleUrl: string
    readonly schemas: readonly ActorSchema[] | undefined
    readonly idleTimeoutMs: number
    readonly createWorker: ActorWorkerFactory
    readonly onIdle: (actor: ResidentActorWorker) => void
    lastCompletedAt = Date.now()
    private readonly onResidencyChange: () => void
    private worker: ActorWorkerHandle | undefined
    private idleTimer: NodeJS.Timeout | undefined

    constructor(options: ResidentActorWorkerOptions) {
        this.moduleUrl = options.moduleUrl
        this.schemas = options.schemas
        this.idleTimeoutMs = options.idleTimeoutMs
        this.createWorker = options.createWorker
        this.onIdle = options.onIdle
        this.onResidencyChange = options.onResidencyChange ?? (() => {})
        this.worker = options.worker
    }

    async execute(
        command: InvokeCommand | WebSocketEventCommand,
        publish?: SocketPublisher,
        connections?: SocketSource
    ): Promise<ActorExecutorReply> {
        if (this.worker === undefined && command.resident_only) return { type: "state_required" }
        if (this.idleTimer !== undefined) clearTimeout(this.idleTimer)
        this.idleTimer = undefined
        this.worker ??= this.createWorker({ moduleUrl: this.moduleUrl, schemas: this.schemas })
        const worker = this.worker
        this.onResidencyChange()
        let reply: ActorExecutorReply
        try {
            reply = await worker.execute(command, publish, connections)
        } catch (error) {
            reply = failedReply(
                error instanceof ActorWorkerTerminatedError ? "actor_worker_terminated" : "actor_worker_failed",
                errorMessage(error)
            )
        } finally {
            this.lastCompletedAt = Date.now()
        }
        if (this.worker !== worker)
            return failedReply("actor_worker_terminated", "resident actor was terminated during invocation")
        if (reply.type === "failed") {
            worker.terminate("actor invocation failed")
            this.worker = undefined
            this.onResidencyChange()
        }
        this.idleTimer = setTimeout(() => this.onIdle(this), this.idleTimeoutMs)
        this.idleTimer.unref()
        return reply
    }

    isResident(): boolean {
        return this.worker !== undefined && (this.worker.isAlive?.() ?? true)
    }

    isIdle(): boolean {
        return this.idleTimer !== undefined
    }

    terminate(reason: string): void {
        if (this.idleTimer !== undefined) clearTimeout(this.idleTimer)
        this.idleTimer = undefined
        this.worker?.terminate(reason)
        this.worker = undefined
        this.onResidencyChange()
    }
}

class ActorWorker implements ActorWorkerHandle {
    isAlive(): boolean {
        return this.terminalError === undefined
    }
    private readonly worker: Worker
    private readonly readyPromise: Promise<readonly string[]>
    private readyResolve: ((actorTypes: readonly string[]) => void) | undefined
    private readyReject: ((error: Error) => void) | undefined
    private replyResolve: ((reply: ActorExecutorReply) => void) | undefined
    private replyReject: ((error: Error) => void) | undefined
    private terminalError: Error | undefined
    private publish: SocketPublisher | undefined
    private connections: SocketSource | undefined

    constructor(data: ActorWorkerData) {
        this.readyPromise = new Promise<readonly string[]>((resolve, reject) => {
            this.readyResolve = resolve
            this.readyReject = reject
        })
        void this.readyPromise.catch(() => undefined)
        this.worker = new Worker(new URL("./actor-worker.js", import.meta.url), { workerData: data })
        this.worker.on("message", (message: ActorWorkerMessage) => this.receive(message))
        this.worker.once("error", error => this.fail(error))
        this.worker.once("exit", code => this.fail(new Error(`actor Worker exited with code ${code}`)))
        this.worker.unref()
    }

    ready(): Promise<readonly string[]> {
        this.worker.ref()
        return this.readyPromise.finally(() => {
            if (this.replyResolve === undefined) this.worker.unref()
        })
    }

    async execute(
        command: InvokeCommand | WebSocketEventCommand,
        publish?: SocketPublisher,
        connections?: SocketSource
    ): Promise<ActorExecutorReply> {
        if (this.terminalError !== undefined) throw this.terminalError
        this.worker.ref()
        this.publish = publish
        this.connections = connections
        try {
            await this.readyPromise
            if (this.terminalError !== undefined) throw this.terminalError
            return await new Promise<ActorExecutorReply>((resolve, reject) => {
                this.replyResolve = resolve
                this.replyReject = reject
                this.post({ type: "execute", command })
            })
        } finally {
            if (this.replyResolve === undefined) this.worker.unref()
        }
    }

    terminate(reason: string): void {
        if (this.terminalError !== undefined) return
        const error = new ActorWorkerTerminatedError(reason)
        this.fail(error)
        void this.worker.terminate()
    }

    private receive(message: ActorWorkerMessage): void {
        if (message.type === "get_connections") {
            void this.loadConnections()
            return
        }
        if (message.type === "socket_effects") {
            void this.publishEffects(message.effects)
            return
        }
        if (message.type === "ready") {
            this.readyResolve?.(message.actorTypes)
            this.readyResolve = undefined
            this.readyReject = undefined
            return
        }
        if (this.readyResolve !== undefined) {
            this.fail(
                new Error(
                    message.type === "failed" ? message.message : `actor Worker sent ${message.type} before ready`
                )
            )
            void this.worker.terminate()
            return
        }
        this.reply(message)
    }

    private async publishEffects(effects: readonly SocketEffect[]): Promise<void> {
        try {
            if (this.publish === undefined) throw new Error("actor socket publishing is unavailable")
            await this.publish(effects)
            this.post({ type: "socket_effects_published" })
        } catch (error) {
            this.post({ type: "socket_effects_published", error: errorMessage(error) })
        }
    }

    private async loadConnections(): Promise<void> {
        try {
            if (this.connections === undefined) throw new Error("actor connection lookup is unavailable")
            const connections = await this.connections()
            this.post({ type: "socket_connections", connections })
        } catch (error) {
            this.post({ type: "socket_connections", connections: [], error: errorMessage(error) })
        }
    }

    private reply(reply: ActorExecutorReply): void {
        const resolve = this.replyResolve
        if (resolve === undefined) return
        this.replyResolve = undefined
        this.replyReject = undefined
        this.publish = undefined
        this.connections = undefined
        resolve(reply)
        this.worker.unref()
    }

    private post(message: ActorWorkerRequest): void {
        this.worker.postMessage(message)
    }

    private fail(error: Error): void {
        if (this.terminalError !== undefined) return
        this.terminalError = error
        this.readyReject?.(error)
        this.readyResolve = undefined
        this.readyReject = undefined
        this.replyReject?.(error)
        this.replyResolve = undefined
        this.replyReject = undefined
        this.worker.unref()
    }
}

class ActorWorkerTerminatedError extends Error {
    constructor(message: string) {
        super(message)
        this.name = "ActorWorkerTerminatedError"
    }
}

export { ActorWorkerSupervisor, DEFAULT_ACTOR_IDLE_TIMEOUT_MS, MAX_RESIDENT_ACTORS }
export type { ResidentActorWorker }
