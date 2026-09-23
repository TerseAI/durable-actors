import { z } from "zod"

import { ActorProtocolError, ActorSerializationError, ActorValidationError } from "./errors.js"
import {
    authorizationHeaders,
    configuredSettings,
    environmentSettings,
    projectActorPath,
    validateActorComponent
} from "./settings.js"

/** Overrides for the backend's DURABLE_ACTORS environment settings. */
interface SocketProxyOptions {
    readonly projectId?: string
    readonly controlPlaneUrl?: string
    readonly apiKey?: string
    /** Defaults to 180000 ms; accepts 1000–600000 ms. */
    readonly setupTimeoutMs?: number
}

interface SocketProxyDependencies {
    readonly fetch?: typeof globalThis.fetch
}

interface ProxyActor<Metadata = unknown> {
    readonly types?: Metadata
}

/** Actor access your backend has already authorized. */
type SocketAuthorization<Actors extends Record<string, ProxyActor>> = {
    [Name in keyof Actors & string]: {
        readonly actorName: Name
        readonly actorId: string
        readonly metadata: Actors[Name] extends ProxyActor<infer Metadata> ? Metadata : never
        readonly homeRegion?: string
        readonly authorizationLifetimeMs?: number
    }
}[keyof Actors & string]

/** Browser connection URL and deadlines in Unix milliseconds. Treat the URL as a credential. */
type SocketGrant = z.infer<typeof socketGrantSchema>

/** Issues browser connection URLs from your backend. */
class SocketProxy<Actors extends Record<string, ProxyActor>> {
    private readonly projectId: string
    private readonly origin: string
    private readonly apiKey: string | undefined
    private readonly setupTimeoutMs: number
    private readonly fetchRequest: typeof globalThis.fetch

    constructor(
        private readonly actors: Actors,
        options: SocketProxyOptions = {},
        dependencies: SocketProxyDependencies = {}
    ) {
        this.setupTimeoutMs = options.setupTimeoutMs ?? 180000
        if (!setupTimeoutSchema.safeParse(this.setupTimeoutMs).success)
            throw new Error("Socket setup timeout must be between one second and ten minutes")
        const settings = configuredSettings(proxySettings(options))
        this.origin = settings.controlPlaneUrl
        this.apiKey = settings.credential
        this.projectId = settings.projectId
        this.fetchRequest = dependencies.fetch ?? globalThis.fetch
    }

    /**
     * Call after authenticating the user and checking actor access.
     * Pass `websocketUrl` to the browser's `new WebSocket()`.
     */
    async handle(authorization: SocketAuthorization<Actors>): Promise<SocketGrant> {
        const actorName = validateActorComponent("actor name", authorization.actorName)
        const actorId = validateActorComponent("actor ID", authorization.actorId)
        if (!Object.hasOwn(this.actors, actorName)) throw new Error(`Unknown actor name: ${actorName}`)
        const metadata = socketMetadata(authorization.metadata)
        const authorizationLifetimeMs = authorization.authorizationLifetimeMs ?? 900000
        if (!authorizationLifetimeSchema.safeParse(authorizationLifetimeMs).success)
            throw new Error("Socket authorization lifetime must be between one second and one day")
        const homeRegion =
            authorization.homeRegion === undefined
                ? undefined
                : validateActorComponent("home region", authorization.homeRegion)
        const response = await this.fetchRequest(
            `${this.origin}${projectActorPath(this.projectId, actorName, actorId)}/find-websocket`,
            {
                method: "POST",
                redirect: "error",
                headers: { ...authorizationHeaders(this.apiKey), "content-type": "application/json" },
                body: JSON.stringify({ metadata, authorizationLifetimeMs, homeRegion }),
                signal: AbortSignal.timeout(this.setupTimeoutMs)
            }
        )
        if (!response.ok) throw new Error(`WebSocket authorization could not be issued (HTTP ${response.status})`)
        return socketGrant(await response.json())
    }
}

function proxySettings(options: SocketProxyOptions) {
    const environment = environmentSettings()
    const controlPlaneUrl = options.controlPlaneUrl ?? environment.controlPlaneUrl
    const apiKey = options.apiKey ?? environment.apiKey
    return {
        projectId: options.projectId ?? environment.projectId,
        controlPlaneUrl: controlPlaneUrl ?? "http://127.0.0.1:7100",
        apiKey
    }
}

export { SocketProxy }
export type { ProxyActor, SocketAuthorization, SocketGrant, SocketProxyDependencies, SocketProxyOptions }

function socketGrant(value: unknown): SocketGrant {
    const grant = socketGrantSchema.safeParse(value)
    if (!grant.success) throw new ActorProtocolError("invalid WebSocket authorization response")
    return grant.data
}

function socketMetadata(value: unknown): unknown {
    let encoded: string
    try {
        encoded = JSON.stringify(value)
        jsonMetadataSchema.parse(value)
    } catch (error) {
        throw new ActorSerializationError("socket metadata must be a JSON value", { cause: error })
    }
    if (new TextEncoder().encode(encoded).length > 64 * 1024)
        throw new ActorValidationError("socket metadata exceeds 65536 bytes")
    return value
}

const setupTimeoutSchema = z.int().min(1000).max(600000)
const authorizationLifetimeSchema = z.int().min(1000).max(86400000)
const socketGrantSchema = z.object({
    websocketUrl: z.url({ protocol: /^wss?$/u }),
    homeRegion: z.string().min(1),
    connectByMs: z.int(),
    authorizedUntilMs: z.int()
})
const jsonMetadataSchema = z.json()
