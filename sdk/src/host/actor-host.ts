import { stat } from "node:fs/promises"
import { type Socket, createConnection } from "node:net"
import path from "node:path"
import { pathToFileURL } from "node:url"
import { z } from "zod"

import type { SocketEffect, SocketLookup, SocketQuery } from "../actor/socketProtocol.js"
import { ActorConfigurationError, ActorProtocolError, ActorSessionError } from "../errors.js"
import type { JsonValue } from "../json.js"

import { ActorExecutor, captureConsole } from "./actor-executor.js"
import type { ActorSessionPort } from "./actor-executor.js"
import { loadActorEntrypoint } from "./actor-loading.js"
import { FileDescriptorDatabaseChannel } from "./database-channel.js"
import type { DatabaseChannel } from "./database-channel.js"
import { parseActorSessionServerMessage } from "./protocol.js"
import type { ActorCallRequest, ActorExecutorReply, ActorSessionClientMessage, LogLevel } from "./protocol.js"
import type { ActorHostSettings } from "./types.js"

async function runActorHost(): Promise<never> {
    const session = new ActorSession()
    await session.start()
    return serveUntilDisconnected(session)
}

// Actor code shares this thread, so a stray throw resets the actor instead of exiting the host.
// Startup failures and the final disconnect still exit the process so Rust sees them.
async function serveUntilDisconnected(session: ActorSession): Promise<never> {
    const fail = (error: unknown) => session.fail(error)
    process.on("uncaughtException", fail).on("unhandledRejection", fail)
    await session.waitUntilDisconnected()
    process.off("uncaughtException", fail).off("unhandledRejection", fail)
    throw new ActorSessionError("Rust host disconnected from actor session")
}

class ActorSession {
    private startup: Promise<void> | undefined
    private connection: ActorSessionConnection | undefined
    private readonly executor: ActorExecutor

    constructor(
        private readonly settings: ActorHostSettings = parseHostSettings(process.env),
        database: DatabaseChannel = databaseChannel(settings),
        private readonly socket?: Socket
    ) {
        this.executor = new ActorExecutor(database)
    }

    start(): Promise<void> {
        this.startup ??= this.initialize()
        return this.startup
    }

    waitUntilDisconnected(): Promise<void> {
        if (this.connection === undefined) throw new ActorSessionError("actor session has not started")
        return this.connection.closed()
    }

    fail(error: unknown): void {
        this.executor.fail(error)
    }

    private async initialize(): Promise<void> {
        const actorEntrypointUrl = await resolveActorEntrypoint(this.settings.actorEntrypoint)
        const releaseConsole = captureConsole(this.executor)
        try {
            const actorNames = await loadActorNames(actorEntrypointUrl, this.settings.startupTimeoutMs)
            this.connection = await ActorSessionConnection.open(
                this.settings.socketPath,
                actorNames,
                this.executor,
                this.settings.startupTimeoutMs,
                this.socket
            )
            void this.connection.closed().then(releaseConsole)
        } catch (error) {
            releaseConsole()
            throw error
        }
    }
}

async function loadActorNames(moduleUrl: string, timeoutMs: number): Promise<readonly string[]> {
    let timer: NodeJS.Timeout | undefined
    try {
        return await Promise.race([
            loadActorEntrypoint(moduleUrl),
            new Promise<never>((_, reject) => {
                timer = setTimeout(
                    () => reject(new ActorSessionError(`actor module loading timed out after ${timeoutMs}ms`)),
                    timeoutMs
                )
            })
        ])
    } finally {
        clearTimeout(timer)
    }
}

class ActorSessionConnection implements ActorSessionPort {
    private unsubscribeResidency: (() => void) | undefined
    private residencyTimer: NodeJS.Timeout | undefined
    private buffer = ""
    private attachedResolve: (() => void) | undefined
    private attachedReject: ((error: Error) => void) | undefined
    private readonly attachedPromise: Promise<void>
    private closedResolve: (() => void) | undefined
    private readonly closedPromise: Promise<void>
    private readonly actorCalls = new Map<
        number,
        { resolve: (outcome: JsonValue) => void; reject: (error: Error) => void }
    >()
    private nextActorCallId = 0
    private readonly loadingConnections = new Map<
        number,
        { resolve: (connections: SocketLookup) => void; reject: (error: Error) => void }
    >()

    static async open(
        socketPath: string,
        actorNames: readonly string[],
        executor: ActorExecutor,
        timeoutMs: number,
        connectedSocket?: Socket
    ): Promise<ActorSessionConnection> {
        if (actorNames.length === 0)
            throw new ActorSessionError("the actor entrypoint does not export any actor classes")
        const socket = connectedSocket ?? (await connectSocket(socketPath))
        const connection = new ActorSessionConnection(socket, executor)
        connection.send({
            type: "attach",
            protocol: 32,
            actor_names: actorNames
        })
        await connection.waitUntilAttached(timeoutMs)
        return connection
    }

    closed(): Promise<void> {
        return this.closedPromise
    }

    allowNextInvocation(messageId: number): void {
        this.send({ type: "ready_for_invocation", message_id: messageId })
    }

    publish(messageId: number, effects: readonly SocketEffect[]): void {
        this.send({ type: "socket_effects", message_id: messageId, effects })
    }

    connections(messageId: number, query?: SocketQuery): Promise<SocketLookup> {
        return new Promise((resolve, reject) => {
            if (this.loadingConnections.has(messageId))
                throw new ActorProtocolError("actor connections are already being loaded")
            this.loadingConnections.set(messageId, { resolve, reject })
            try {
                this.send({ type: "get_connections", message_id: messageId, query })
            } catch (error) {
                this.loadingConnections.delete(messageId)
                reject(sessionError(error))
            }
        })
    }

    callActor(call: ActorCallRequest): Promise<JsonValue> {
        return new Promise((resolve, reject) => {
            if (this.socket.destroyed) throw new ActorSessionError("Rust host disconnected before the actor call")
            const callId = ++this.nextActorCallId
            this.actorCalls.set(callId, { resolve, reject })
            try {
                this.send({ type: "actor_call", call_id: callId, call })
            } catch (error) {
                this.actorCalls.delete(callId)
                reject(sessionError(error))
            }
        })
    }

    log(messageId: number | undefined, level: LogLevel, message: string): void {
        this.send({ type: "log", message_id: messageId, level, message })
    }

    private constructor(
        private readonly socket: Socket,
        private readonly executor: ActorExecutor
    ) {
        this.attachedPromise = new Promise<void>((resolve, reject) => {
            this.attachedResolve = resolve
            this.attachedReject = reject
        })
        this.closedPromise = new Promise<void>(resolve => {
            this.closedResolve = resolve
        })
        this.bindSocket()
    }

    private waitUntilAttached(timeoutMs: number): Promise<void> {
        return new Promise<void>((resolve, reject) => {
            const timeout = setTimeout(() => {
                const error = new ActorSessionError(`actor session attachment timed out after ${timeoutMs}ms`)
                this.fail(error)
                reject(error)
            }, timeoutMs)
            void this.attachedPromise.then(
                () => {
                    clearTimeout(timeout)
                    resolve()
                },
                error => {
                    clearTimeout(timeout)
                    reject(error)
                }
            )
        })
    }

    private bindSocket(): void {
        this.socket.setEncoding("utf8")
        this.socket.on("data", (chunk: string) => this.acceptChunk(chunk))
        this.socket.once("error", error => this.fail(error))
        this.socket.once("close", () => this.close())
    }

    private acceptChunk(chunk: string): void {
        this.buffer += chunk
        let newline = this.buffer.indexOf("\n")
        while (newline !== -1) {
            const rawMessage = this.buffer.slice(0, newline)
            this.buffer = this.buffer.slice(newline + 1)
            void this.handle(rawMessage)
            newline = this.buffer.indexOf("\n")
        }
    }

    private async handle(rawMessage: string): Promise<void> {
        try {
            const message = parseActorSessionServerMessage(rawMessage)
            switch (message.type) {
                case "attached":
                    this.executor.assign(message.actor, this)
                    this.reportResidency()
                    this.attachedResolve?.()
                    this.attachedResolve = undefined
                    this.attachedReject = undefined
                    break
                case "command":
                    this.reply(message.message_id, await this.executor.handle(message.message_id, message.command))
                    break
                case "actor_result": {
                    const pending = this.actorCalls.get(message.call_id)
                    if (pending === undefined) throw new ActorProtocolError("Rust host replied to unknown actor call")
                    this.actorCalls.delete(message.call_id)
                    pending.resolve(message.outcome)
                    break
                }
                case "socket_connections": {
                    const pending = this.loadingConnections.get(message.message_id)
                    if (pending === undefined)
                        throw new ActorProtocolError("Rust host replied to unknown connection lookup")
                    this.loadingConnections.delete(message.message_id)
                    if (message.error === undefined) pending.resolve(message.connections)
                    else pending.reject(new ActorSessionError(message.error))
                    break
                }
                default:
                    throw message satisfies never
            }
        } catch (error) {
            this.fail(sessionError(error))
        }
    }

    private reportResidency(): void {
        if (this.residencyTimer) return
        const report = () => {
            try {
                this.send({ type: "residency", resident: this.executor.isResident() })
            } catch (error) {
                this.fail(sessionError(error))
            }
        }
        this.unsubscribeResidency = this.executor.onResidencyChange(report)
        report()
        this.residencyTimer = setInterval(report, 1_000)
        this.residencyTimer.unref()
    }

    private reply(messageId: number, reply: ActorExecutorReply): void {
        this.send({ type: "reply", message_id: messageId, reply })
    }

    private send(message: ActorSessionClientMessage): void {
        this.socket.write(`${JSON.stringify(message)}\n`)
    }

    private fail(error: Error): void {
        this.attachedReject?.(error)
        this.attachedResolve = undefined
        this.attachedReject = undefined
        this.socket.destroy()
    }

    private close(): void {
        clearInterval(this.residencyTimer)
        this.unsubscribeResidency?.()
        this.unsubscribeResidency = undefined
        for (const pending of this.actorCalls.values())
            pending.reject(new ActorSessionError("Rust host disconnected during an actor call"))
        this.actorCalls.clear()
        for (const pending of this.loadingConnections.values())
            pending.reject(new ActorSessionError("Rust host disconnected while loading connections"))
        this.loadingConnections.clear()
        this.attachedReject?.(new ActorSessionError("Rust host disconnected from actor session"))
        this.attachedResolve = undefined
        this.attachedReject = undefined
        this.closedResolve?.()
        this.closedResolve = undefined
    }
}

function connectSocket(socketPath: string): Promise<Socket> {
    return new Promise((resolve, reject) => {
        const socket = createConnection(socketPath)
        const onError = (error: Error): void => {
            socket.off("connect", onConnect)
            socket.destroy()
            reject(new ActorSessionError(`could not attach to Rust host at ${socketPath}`, { cause: error }))
        }
        const onConnect = (): void => {
            socket.off("error", onError)
            resolve(socket)
        }
        socket.once("error", onError)
        socket.once("connect", onConnect)
    })
}

function sessionError(error: unknown): Error {
    return error instanceof Error ? error : new ActorSessionError(String(error))
}

function databaseChannel(settings: ActorHostSettings): DatabaseChannel {
    if (settings.databaseFd === undefined)
        throw new ActorConfigurationError("DURABLE_ACTORS_DATABASE_FD must name the Rust host's database channel")
    return new FileDescriptorDatabaseChannel(settings.databaseFd)
}

async function resolveActorEntrypoint(configured: string | undefined): Promise<string> {
    const entrypointPath = path.resolve(configured ?? DEFAULT_ACTOR_ENTRYPOINT)
    await requireFile(
        entrypointPath,
        configured === undefined
            ? `default actor entrypoint ${DEFAULT_ACTOR_ENTRYPOINT}`
            : `configured actor entrypoint ${configured}`
    )
    return pathToFileURL(entrypointPath).href
}

async function requireFile(filePath: string, label: string): Promise<void> {
    if (!(await isFile(filePath))) throw new ActorConfigurationError(`${label} is not a file`)
}

async function isFile(filePath: string): Promise<boolean> {
    try {
        return (await stat(filePath)).isFile()
    } catch {
        return false
    }
}

function parseHostSettings(environment: NodeJS.ProcessEnv): ActorHostSettings {
    const result = actorSessionSettingsSchema.safeParse(environment)
    if (!result.success)
        throw new ActorConfigurationError(`actor-host session settings are invalid: ${result.error.message}`)
    return {
        socketPath: result.data.DURABLE_ACTORS_EXECUTOR_SOCKET,
        actorEntrypoint: result.data.DURABLE_ACTORS_ENTRYPOINT,
        databaseFd: result.data.DURABLE_ACTORS_DATABASE_FD,
        startupTimeoutMs: parseStartupTimeout(environment.DURABLE_ACTORS_HOST_STARTUP_MS)
    }
}

function parseStartupTimeout(value: string | undefined): number {
    if (value === undefined) return DEFAULT_ACTOR_STARTUP_TIMEOUT_MS
    const parsed = Number(value)
    if (!Number.isInteger(parsed) || parsed <= 0)
        throw new ActorConfigurationError("DURABLE_ACTORS_HOST_STARTUP_MS must be a positive integer")
    return parsed
}

const DEFAULT_ACTOR_STARTUP_TIMEOUT_MS = 10_000

const actorSessionSettingsSchema = z.object({
    DURABLE_ACTORS_EXECUTOR_SOCKET: z.string().trim().min(1),
    DURABLE_ACTORS_ENTRYPOINT: z.string().trim().min(1).optional(),
    DURABLE_ACTORS_DATABASE_FD: z.coerce.number().int().nonnegative().optional()
})

const DEFAULT_ACTOR_ENTRYPOINT = "dist/actors.mjs"

export { ActorSession, connectSocket, parseHostSettings, resolveActorEntrypoint, runActorHost, serveUntilDisconnected }
