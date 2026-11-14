import { AsyncLocalStorage } from "node:async_hooks"
import { format } from "node:util"

import { findActorDefinition } from "../actor/actor.js"
import type { ActorIdentity } from "../actor/identity.js"
import type { SocketEffect, SocketLookup, SocketQuery } from "../actor/socketProtocol.js"
import { remoteActorClient, runWithActorClient } from "../client/client.js"
import { errorMessage } from "../errors.js"
import type { JsonValue } from "../json.js"

import { ActorRuntime } from "./actor-runtime.js"
import type { DatabaseChannel } from "./database-channel.js"
import { DatabaseProxy } from "./database.js"
import { HostActorClient } from "./host-actor-client.js"
import { failedReply } from "./protocol.js"
import type { ActorCallRequest, ActorExecutorCommand, ActorExecutorReply, EvictCommand, LogLevel } from "./protocol.js"

/** What running actor code needs from the Rust host session. */
interface ActorSessionPort {
    allowNextInvocation(messageId: number): void
    publish(messageId: number, effects: readonly SocketEffect[]): void
    connections(messageId: number, query?: SocketQuery): Promise<SocketLookup>
    callActor(call: ActorCallRequest): Promise<JsonValue>
    log(messageId: number | undefined, level: LogLevel, message: string): void
}

interface Resident {
    readonly runtime: ActorRuntime
    readonly sequenceBase: number
}

/** Runs the assigned actor on this thread and rebuilds its instance after eviction or failure. */
class ActorExecutor {
    private readonly invocation = new AsyncLocalStorage<number>()
    private readonly actorClient = new HostActorClient(call => this.session().callActor(call), remoteActorClient)
    private readonly residencyListeners = new Set<() => void>()
    private actor: ActorIdentity | undefined
    private port: ActorSessionPort | undefined
    private resident: Resident | undefined
    private lastSequence = 0

    constructor(private readonly database: DatabaseChannel) {}

    assign(actor: ActorIdentity, port: ActorSessionPort): void {
        if (findActorDefinition(actor.actor_name) === undefined)
            throw new Error(`actor name ${actor.actor_name} is not loaded in this customer process`)
        this.actor = { ...actor }
        this.port = port
    }

    async handle(messageId: number, command: ActorExecutorCommand): Promise<ActorExecutorReply> {
        if (this.actor === undefined) return failedReply("actor_executor_failed", "no actor is assigned")
        if (command.type === "evict") return this.evict(messageId, command, this.actor)
        const actor = this.actor
        const resident = this.resident ?? this.createResident(actor)
        const reply = await this.run(messageId, () => resident.runtime.handle({ ...command, actor }))
        if (reply.type === "failed" && !RECOVERABLE_FAILURES.has(reply.code)) this.discard(resident, reply.message)
        return this.ordered(reply, resident.sequenceBase)
    }

    isResident(): boolean {
        return this.resident !== undefined
    }

    onResidencyChange(listener: () => void): () => void {
        this.residencyListeners.add(listener)
        return () => {
            this.residencyListeners.delete(listener)
        }
    }

    log(level: LogLevel, message: string): void {
        if (this.port === undefined) writeToStdio(level, message)
        else this.port.log(this.invocation.getStore(), level, message)
    }

    /** Drops the instance after an error escaped actor code, so Rust resets it from durable state. */
    fail(error: unknown): void {
        this.log("error", `actor code threw outside an invocation: ${format(error)}`)
        if (this.resident !== undefined) this.discard(this.resident, errorMessage(error))
    }

    private async evict(messageId: number, command: EvictCommand, actor: ActorIdentity): Promise<ActorExecutorReply> {
        const resident = this.resident
        if (resident === undefined) return { type: "evicted" }
        await this.run(messageId, () => resident.runtime.handle({ ...command, actor }))
        this.discard(resident, "resident actor was evicted by the Rust host")
        return { type: "evicted" }
    }

    private createResident(actor: ActorIdentity): Resident {
        const runtime = new ActorRuntime(
            findActorDefinition(actor.actor_name)!,
            () => this.session().allowNextInvocation(this.currentInvocation()),
            new DatabaseProxy(call => this.database.call(call, this.invocation.getStore())),
            effects => this.session().publish(this.currentInvocation(), effects),
            async query => this.session().connections(this.currentInvocation(), query)
        )
        this.resident = { runtime, sequenceBase: this.lastSequence }
        this.notifyResidency()
        return this.resident
    }

    // Invocations still running on a discarded instance fail instead of committing.
    private discard(resident: Resident, reason: string): void {
        resident.runtime.fail(failedReply("actor_executor_terminated", reason))
        if (this.resident !== resident) return
        this.resident = undefined
        this.notifyResidency()
    }

    private async run(messageId: number, operation: () => Promise<ActorExecutorReply>): Promise<ActorExecutorReply> {
        try {
            return await this.invocation.run(messageId, () => runWithActorClient(this.actorClient, operation))
        } catch (error) {
            return failedReply("actor_executor_failed", errorMessage(error))
        }
    }

    private ordered(reply: ActorExecutorReply, sequenceBase: number): ActorExecutorReply {
        if ((reply.type !== "invoked" && reply.type !== "websocket_handled") || reply.sequence === undefined)
            return reply
        const sequence = sequenceBase + reply.sequence
        this.lastSequence = Math.max(this.lastSequence, sequence)
        return { ...reply, sequence }
    }

    private notifyResidency(): void {
        for (const listener of this.residencyListeners) listener()
    }

    private session(): ActorSessionPort {
        if (this.port === undefined) throw new Error("actor executor has no Rust host session")
        return this.port
    }

    private currentInvocation(): number {
        const messageId = this.invocation.getStore()
        if (messageId === undefined) throw new Error("actor host call has no active invocation")
        return messageId
    }
}

/** Routes console output through the executor, tagged with the invocation that wrote it. */
function captureConsole(executor: ActorExecutor): () => void {
    const original = Object.fromEntries(CONSOLE_LEVELS.map(([method]) => [method, console[method]]))
    for (const [method, level] of CONSOLE_LEVELS)
        console[method] = (...args: unknown[]) => executor.log(level, truncateLog(format(...args)))
    return () => Object.assign(console, original)
}

function truncateLog(message: string): string {
    if (message.length * 3 <= LOG_LIMIT_BYTES) return message
    const bytes = Buffer.from(message)
    return bytes.length <= LOG_LIMIT_BYTES ? message : bytes.subarray(0, LOG_LIMIT_BYTES).toString()
}

// Before the session attaches there is no socket, so output keeps going to the terminal.
function writeToStdio(level: LogLevel, message: string): void {
    const stream = level === "warn" || level === "error" ? process.stderr : process.stdout
    stream.write(`${message}\n`)
}

const RECOVERABLE_FAILURES = new Set([
    "actor_method_failed",
    "actor_socket_failed",
    "method_not_found",
    "method_not_callable"
])

const LOG_LIMIT_BYTES = 256 * 1024

const CONSOLE_LEVELS = [
    ["debug", "debug"],
    ["log", "info"],
    ["info", "info"],
    ["warn", "warn"],
    ["error", "error"]
] as const satisfies readonly (readonly [keyof Console, LogLevel])[]

export { ActorExecutor, captureConsole }
export type { ActorSessionPort }
