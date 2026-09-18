import { performance } from "node:perf_hooks"
import WebSocket from "ws"
import { z } from "zod"

import { validateActorComponent } from "../actor/identity.js"
import { currentActorInvocation } from "../actor/invocationContext.js"
import { socketMessage } from "../actor/socket.js"
import type { ActorConnection, ActorSocketMessage } from "../actor/socket.js"
import type { SocketEffect } from "../actor/socketProtocol.js"
import { socketMetadata } from "../actor/socketValidation.js"
import type { ActorSchemas } from "../actor/socketValidation.js"
import { ActorInvocationError, ActorProtocolError } from "../errors.js"
import { cloneJson } from "../json.js"
import type { JsonValue } from "../json.js"

import { GrpcActorHostTransport } from "./actorHostGrpc.js"
import type { ActorHostTarget, ActorHostTransport, DirectActorInvocation } from "./actorHostGrpc.js"
import { configuredSettings } from "./clientSettings.js"
import { SocketConnection } from "./socketConnection.js"
import { LatencyTimeline, stderrTelemetry } from "./telemetry.js"
import type { TelemetrySink } from "./telemetry.js"

const TARGET_EXPIRATION_SAFETY_MS = 5_000

class RemoteActorClient {
    private settingsValue: RemoteActorSettings | undefined
    private readonly environment: NodeJS.ProcessEnv
    private readonly fetchRequest: typeof globalThis.fetch
    private readonly requestId: () => string
    private readonly actorHost: ActorHostTransport
    private readonly now: () => number
    private readonly monotonicNow: () => number
    private readonly telemetry: TelemetrySink
    private readonly targets = new Map<string, Promise<ActorHostTarget>>()
    private readonly connectWebSocket: WebSocketConnector

    constructor(options?: DurableObjectsClientOptions, dependencies: RemoteActorClientDependencies = {}) {
        this.environment = dependencies.environment ?? process.env
        this.fetchRequest = dependencies.fetch ?? globalThis.fetch
        this.requestId = dependencies.requestId ?? (() => globalThis.crypto.randomUUID())
        this.actorHost = dependencies.actorHost ?? new GrpcActorHostTransport()
        // Workflow runtimes can replace Date.now with replayable logical time.
        this.now = dependencies.now ?? (() => performance.timeOrigin + performance.now())
        this.monotonicNow = dependencies.monotonicNow ?? (() => performance.now())
        this.telemetry = dependencies.telemetry ?? stderrTelemetry
        this.connectWebSocket = dependencies.connectWebSocket ?? openWebSocket
        this.settingsValue = options === undefined ? undefined : configuredSettings(options)
    }

    async invoke(actorType: string, actorId: string, method: string, args: readonly unknown[]): Promise<unknown> {
        const requestId = validateActorComponent("request ID", this.requestId())
        const timeline = new LatencyTimeline(this.monotonicNow)
        let outcome = "failed"
        try {
            if (currentActorInvocation() !== undefined) {
                throw new ActorInvocationError("actor_error", requestId, "actor-to-actor calls are not available")
            }
            const invocation = this.invocation(requestId, actorType, actorId, method, args)
            timeline.mark("invocation_built")
            const target = await this.target(invocation, timeline)
            timeline.mark("target_resolved")
            const result = await this.direct(target, invocation, true, timeline)
            outcome = "completed"
            return result
        } finally {
            this.telemetry({
                event: "actor_client_invocation",
                request_id: requestId,
                actor_type: actorType,
                actor_id: actorId,
                method,
                ...timeline.finish(),
                outcome
            })
        }
    }

    async connect(
        actorType: string,
        actorId: string,
        metadata: unknown,
        schemas: ActorSchemas = {}
    ): Promise<ActorConnection> {
        const requestId = validateActorComponent("request ID", this.requestId())
        if (currentActorInvocation() !== undefined)
            throw new ActorInvocationError(
                "actor_error",
                requestId,
                "actor-to-actor socket connections are not available"
            )
        const actor = {
            actorType: validateActorComponent("actor type", actorType),
            actorId: validateActorComponent("actor ID", actorId)
        }
        const attachment = socketMetadata(metadata, schemas)
        const response = await this.fetchRequest(
            `${this.settings.controlPlaneUrl}${actorPath(actor.actorType, actor.actorId)}/connect`,
            {
                method: "POST",
                headers: { authorization: `Bearer ${this.settings.credential}`, "content-type": "application/json" },
                body: JSON.stringify({
                    transport: "websocket",
                    metadata: attachment,
                    backend: true,
                    homeRegion: this.settings.homeRegion
                }),
                signal: AbortSignal.timeout(180000)
            }
        )
        if (!response.ok)
            throw new ActorInvocationError("unavailable", requestId, `Socket setup returned HTTP ${response.status}`)
        const grant = z
            .object({
                websocketUrl: z.url().refine(url => ["ws:", "wss:"].includes(new URL(url).protocol))
            })
            .parse(await response.json())
        return this.connectWebSocket(grant.websocketUrl, attachment, schemas)
    }

    async broadcast(actorType: string, actorId: string, message: ActorSocketMessage): Promise<void> {
        const requestId = validateActorComponent("request ID", this.requestId())
        if (currentActorInvocation() !== undefined)
            throw new ActorInvocationError(
                "actor_error",
                requestId,
                "actor-to-actor socket broadcasts are not available"
            )
        const actor = {
            requestId,
            actorType: validateActorComponent("actor type", actorType),
            actorId: validateActorComponent("actor ID", actorId)
        }
        const target = await this.target(actor, new LatencyTimeline(this.monotonicNow))
        await this.deliverSocketEffects(
            target,
            actor,
            [{ type: "broadcast", message: socketMessage(message), except_connection_ids: [], tags: [] }],
            "actor socket broadcast"
        )
    }

    private async direct(
        target: ActorHostTarget,
        invocation: DirectActorInvocation,
        retryReroute: boolean,
        timeline: LatencyTimeline
    ): Promise<unknown> {
        try {
            const reply = await this.actorHost.invoke(target, invocation)
            timeline.mark("host_rpc_completed")
            if (reply.type === "completed") {
                await this.applySocketEffects(target, invocation, reply.effects)
                timeline.mark("socket_effects_completed")
                return reply.result
            }
            if (reply.type === "failed") throw new ActorInvocationError(reply.code, invocation.requestId, reply.message)
            if (reply.type === "unauthenticated" && !retryReroute) {
                this.targets.delete(actorKey(invocation.actorType, invocation.actorId))
                throw new ActorInvocationError(
                    "unauthenticated",
                    invocation.requestId,
                    "actor host rejected the refreshed invocation ticket"
                )
            }
            if (!retryReroute)
                throw new ActorInvocationError(
                    "unavailable",
                    invocation.requestId,
                    "actor ownership changed repeatedly before execution"
                )
            this.targets.delete(actorKey(invocation.actorType, invocation.actorId))
            const rerouted = await this.target(invocation, timeline)
            return this.direct(rerouted, invocation, false, timeline)
        } catch (error) {
            if (error instanceof ActorInvocationError || error instanceof ActorProtocolError) throw error
            this.targets.delete(actorKey(invocation.actorType, invocation.actorId))
            const message = error instanceof Error ? error.message : String(error)
            throw new ActorInvocationError(
                "outcome_unknown",
                invocation.requestId,
                `actor-host gRPC request failed after dispatch: ${message}`
            )
        }
    }

    private async applySocketEffects(
        target: ActorHostTarget,
        invocation: DirectActorInvocation,
        effects: readonly SocketEffect[]
    ): Promise<void> {
        if (effects.length === 0) return
        await this.deliverSocketEffects(target, invocation, effects, "actor completed but socket effects")
    }

    private async deliverSocketEffects(
        target: ActorHostTarget,
        actor: ActorAddress,
        effects: readonly SocketEffect[],
        context: string
    ): Promise<void> {
        try {
            await this.actorHost.publish(target, actor, effects)
        } catch (error) {
            this.targets.delete(actorKey(actor.actorType, actor.actorId))
            const message = error instanceof Error ? error.message : String(error)
            throw new ActorInvocationError(
                "outcome_unknown",
                actor.requestId,
                `${context} could not be delivered: ${message}`
            )
        }
    }

    private async target(invocation: ActorAddress, timeline: LatencyTimeline): Promise<ActorHostTarget> {
        const key = actorKey(invocation.actorType, invocation.actorId)
        const current = this.targets.get(key)
        timeline.mark("target_cache_checked")
        if (current) {
            const target = await current
            if (target.expiresAtMs > this.now() + TARGET_EXPIRATION_SAFETY_MS) return target
            this.targets.delete(key)
        }
        const resolving = this.resolveTarget(invocation).catch(error => {
            this.targets.delete(key)
            throw error
        })
        this.targets.set(key, resolving)
        return resolving
    }

    private async resolveTarget(invocation: ActorAddress): Promise<ActorHostTarget> {
        let response: Response
        try {
            response = await this.fetchRequest(targetUrl(this.settings, invocation.actorType, invocation.actorId), {
                method: "POST",
                headers: {
                    accept: "application/json",
                    authorization: `Bearer ${this.settings.credential}`,
                    "x-request-id": invocation.requestId,
                    "content-type": "application/json"
                },
                body: JSON.stringify({ transport: "grpc", homeRegion: this.settings.homeRegion })
            })
        } catch (error) {
            const message = error instanceof Error ? error.message : String(error)
            throw new ActorInvocationError(
                "outcome_unknown",
                invocation.requestId,
                `control-plane HTTP request failed before dispatch: ${message}`
            )
        }
        const document = await responseDocument(response)
        if (!response.ok) this.throwResponseFailure(response, document, invocation.requestId)
        const target = actorHostTargetSchema.safeParse(document)
        if (!target.success)
            throw new ActorProtocolError("control-plane response did not contain a valid actor host target")
        return target.data
    }

    private throwResponseFailure(response: Response, document: unknown, requestId: string): never {
        if (response.status === 401 || response.status === 403) {
            throw new ActorInvocationError(
                "unauthenticated",
                requestId,
                "the durable-object application credential was rejected"
            )
        }
        const failure = errorDocumentSchema.safeParse(document)
        if (!failure.success) {
            throw new ActorProtocolError(`control-plane HTTP ${response.status} response did not contain a valid error`)
        }
        throw new ActorInvocationError(
            failure.data.error.code,
            failure.data.error.requestId ?? requestId,
            failure.data.error.message
        )
    }

    private invocation(
        requestId: string,
        actorType: string,
        actorId: string,
        method: string,
        args: readonly unknown[]
    ): DirectActorInvocation {
        return {
            requestId,
            actorType: validateActorComponent("actor type", actorType),
            actorId: validateActorComponent("actor ID", actorId),
            method: validateActorComponent("actor method", method),
            args: this.jsonArguments(args)
        }
    }

    private get settings(): RemoteActorSettings {
        if (this.settingsValue !== undefined) return this.settingsValue
        this.settingsValue = configuredSettings({
            apiKey: this.environment.DURABLE_OBJECT_API_KEY,
            homeRegion: this.environment.DURABLE_OBJECT_HOME_REGION,
            controlPlaneUrl: this.environment.DURABLE_OBJECT_CONTROL_PLANE_URL ?? "http://127.0.0.1:7100"
        })
        return this.settingsValue
    }

    private jsonArguments(args: readonly unknown[]): readonly JsonValue[] {
        const value = cloneJson(args, "actor arguments")
        if (!Array.isArray(value)) throw new ActorProtocolError("actor arguments must be a JSON array")
        return value
    }
}

function targetUrl(settings: RemoteActorSettings, actorType: string, actorId: string): string {
    const actor = validateActorComponent("actor type", actorType)
    const id = validateActorComponent("actor ID", actorId)
    return `${settings.controlPlaneUrl}${actorPath(actor, id)}/connect`
}

function actorPath(actorType: string, actorId: string): string {
    return `/v1/actors/${encodeURIComponent(actorType)}/${encodeURIComponent(actorId)}`
}

function openWebSocket(url: string, metadata: JsonValue, schemas: ActorSchemas): Promise<ActorConnection> {
    const socket = new WebSocket(url)
    const connection = new SocketConnection(socket, schemas)
    return new Promise((resolve, reject) => {
        let opened = false
        socket.addEventListener(
            "open",
            () => {
                try {
                    socket.send(JSON.stringify({ type: "initialize", metadata }))
                    opened = true
                    resolve(connection)
                } catch (error) {
                    socket.close()
                    reject(error)
                }
            },
            { once: true }
        )
        socket.addEventListener("error", () => {
            if (!opened) reject(new Error("actor WebSocket connection failed"))
        })
        socket.addEventListener("close", () => {
            if (!opened) reject(new Error("actor WebSocket closed before connecting"))
        })
    })
}

function actorKey(actorType: string, actorId: string): string {
    return `${actorType}\u001f${actorId}`
}

async function responseDocument(response: Response): Promise<unknown> {
    try {
        return (await response.json()) as unknown
    } catch (error) {
        throw new ActorProtocolError(`control-plane HTTP ${response.status} response was not valid JSON`, {
            cause: error
        })
    }
}

type ActorAddress = Pick<DirectActorInvocation, "requestId" | "actorType" | "actorId">

interface RemoteActorSettings {
    readonly credential: string
    readonly homeRegion?: string
    readonly controlPlaneUrl: string
}

interface DurableObjectsClientOptions {
    readonly apiKey: string
    readonly homeRegion?: string
    readonly controlPlaneUrl: string
}

interface RemoteActorClientDependencies {
    readonly environment?: NodeJS.ProcessEnv
    readonly fetch?: typeof globalThis.fetch
    readonly requestId?: () => string
    readonly actorHost?: ActorHostTransport
    readonly now?: () => number
    readonly monotonicNow?: () => number
    readonly telemetry?: TelemetrySink
    readonly connectWebSocket?: WebSocketConnector
}

type WebSocketConnector = (url: string, metadata: JsonValue, schemas: ActorSchemas) => Promise<ActorConnection>

const errorDocumentSchema = z.object({
    error: z.object({
        code: z.string().min(1),
        message: z.string(),
        requestId: z.string().optional()
    })
})

const actorHostTargetSchema = z.object({
    route: z.string().url(),
    token: z.string().trim().min(1),
    ownerEpoch: z.number().int().positive(),

    expiresAtMs: z.number().int().positive()
})

export { RemoteActorClient }
export type { DurableObjectsClientOptions, RemoteActorClientDependencies }
