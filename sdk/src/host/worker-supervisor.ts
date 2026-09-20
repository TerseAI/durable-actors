import { Worker } from "node:worker_threads"

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
    HydrateCommand,
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

class ActorWorkerSupervisor {
    private readonly actorEntrypointUrl: string
    private readonly actorSchemas: readonly ActorSchema[] | undefined
    private readonly actorIdleTimeoutMs: number
    private readonly createWorker: ActorWorkerFactory
    private resident: ResidentActorWorker | undefined
    private speculativeWorker: ActorWorkerHandle | undefined
    private speculativeTimer: NodeJS.Timeout | undefined
    private identity: string | undefined
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
        const identity = actorKey(command.actor)
        if (this.identity !== undefined && this.identity !== identity)
            return failedReply("actor_identity_mismatch", "sandbox is permanently assigned to another actor")
        if (command.type !== "evict") this.identity ??= identity
        switch (command.type) {
            case "hydrate":
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

    close(): void {
        this.closed = true
        this.takeSpeculativeWorker()?.terminate("actor supervisor is closed")
        this.resident?.terminate("actor supervisor is closed")
        this.resident = undefined
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
        command: InvokeCommand | WebSocketEventCommand | HydrateCommand,
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
        let actor = this.resident
        if (actor === undefined) {
            if (command.resident_only) return Promise.resolve({ type: "state_required" })
            actor = new ResidentActorWorker({
                moduleUrl: this.actorEntrypointUrl,
                schemas: this.actorSchemas,
                idleTimeoutMs: this.actorIdleTimeoutMs,
                worker: this.takeSpeculativeWorker(),
                createWorker: this.createWorker,
                onIdle: candidate => this.removeIfCurrent(candidate)
            })
            this.resident = actor
        }
        return actor.execute(command, publish, connections)
    }

    private evict(_command: EvictCommand): ActorExecutorReply {
        this.resident?.terminate("resident actor was evicted by the Rust host")
        this.resident = undefined
        return { type: "evicted" }
    }

    private removeIfCurrent(actor: ResidentActorWorker): void {
        if (this.resident !== actor || !actor.isIdle()) return
        this.resident = undefined
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
    private worker: ActorWorkerHandle | undefined
    private idleTimer: NodeJS.Timeout | undefined

    constructor(options: ResidentActorWorkerOptions) {
        this.moduleUrl = options.moduleUrl
        this.schemas = options.schemas
        this.idleTimeoutMs = options.idleTimeoutMs
        this.createWorker = options.createWorker
        this.onIdle = options.onIdle
        this.worker = options.worker
    }

    async execute(
        command: InvokeCommand | WebSocketEventCommand | HydrateCommand,
        publish?: SocketPublisher,
        connections?: SocketSource
    ): Promise<ActorExecutorReply> {
        if (this.worker === undefined && command.resident_only) return { type: "state_required" }
        if (this.idleTimer !== undefined) clearTimeout(this.idleTimer)
        this.idleTimer = undefined
        this.worker ??= this.createWorker({ moduleUrl: this.moduleUrl, schemas: this.schemas })
        const worker = this.worker
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
        }
        this.idleTimer = setTimeout(() => this.onIdle(this), this.idleTimeoutMs)
        this.idleTimer.unref()
        return reply
    }

    isIdle(): boolean {
        return this.idleTimer !== undefined
    }

    terminate(reason: string): void {
        if (this.idleTimer !== undefined) clearTimeout(this.idleTimer)
        this.idleTimer = undefined
        this.worker?.terminate(reason)
        this.worker = undefined
    }
}

class ActorWorker implements ActorWorkerHandle {
    private readonly worker: Worker
    private readonly readyPromise: Promise<readonly string[]>
    private readyResolve: ((actorTypes: readonly string[]) => void) | undefined
    private readyReject: ((error: Error) => void) | undefined
    private replyResolve: ((reply: ActorExecutorReply) => void) | undefined
    private replyReject: ((error: Error) => void) | undefined
    private terminalError: Error | undefined
    private publish: SocketPublisher | undefined
    private connections: SocketSource | undefined

    private readonly warmPromise: Promise<void>
    private warmResolve: (() => void) | undefined
    private warmReject: ((error: Error) => void) | undefined
    private assigned: boolean

    constructor(data?: ActorWorkerData) {
        this.assigned = data !== undefined
        this.warmPromise = new Promise((resolve, reject) => {
            this.warmResolve = resolve
            this.warmReject = reject
        })
        void this.warmPromise.catch(() => undefined)
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

    warm(): Promise<void> {
        this.worker.ref()
        return this.warmPromise
    }

    load(data: ActorWorkerData): void {
        if (this.assigned) throw new Error("customer code already assigned")
        this.assigned = true
        this.post({ type: "load", data })
    }

    ready(): Promise<readonly string[]> {
        this.worker.ref()
        return this.readyPromise.finally(() => {
            if (this.replyResolve === undefined) this.worker.unref()
        })
    }

    async execute(
        command: InvokeCommand | WebSocketEventCommand | HydrateCommand,
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
        if (message.type === "warm") {
            this.warmResolve?.()
            this.warmResolve = undefined
            this.warmReject = undefined
            return
        }
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
        this.warmReject?.(error)
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

export { ActorWorker, ActorWorkerSupervisor, DEFAULT_ACTOR_IDLE_TIMEOUT_MS }
export type { ResidentActorWorker }
