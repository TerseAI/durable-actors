/** @module durable-actors/proxy */
import { z } from "zod"

import { projectActorPath, validateActorComponent } from "./actor/identity.js"
import { socketMetadata } from "./actor/socketValidation.js"
import { authorizationHeaders, configuredSettings } from "./client/clientSettings.js"
import { actorEnvironment } from "./environment.js"

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

const socketGrantSchema = z.object({
    websocketUrl: z.url().refine(url => ["ws:", "wss:"].includes(new URL(url).protocol)),
    homeRegion: z.string().min(1),
    connectByMs: z.number().int(),
    authorizedUntilMs: z.number().int()
})

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
        if (!Number.isSafeInteger(this.setupTimeoutMs) || this.setupTimeoutMs < 1000 || this.setupTimeoutMs > 600000)
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
        if (
            !Number.isSafeInteger(authorizationLifetimeMs) ||
            authorizationLifetimeMs < 1000 ||
            authorizationLifetimeMs > 86400000
        )
            throw new Error("Socket authorization lifetime must be between one second and one day")
        const homeRegion =
            authorization.homeRegion === undefined
                ? undefined
                : validateActorComponent("home region", authorization.homeRegion)
        const response = await this.fetchRequest(
            `${this.origin}${projectActorPath(this.projectId, actorName, actorId)}/find-websocket`,
            {
                method: "POST",
                headers: { ...authorizationHeaders(this.apiKey), "content-type": "application/json" },
                body: JSON.stringify({ metadata, authorizationLifetimeMs, homeRegion }),
                signal: AbortSignal.timeout(this.setupTimeoutMs)
            }
        )
        if (!response.ok) throw new Error(`WebSocket authorization could not be issued (HTTP ${response.status})`)
        return socketGrantSchema.parse(await response.json())
    }
}

function proxySettings(options: SocketProxyOptions) {
    const environment = actorEnvironment(process.env)
    const controlPlaneUrl = options.controlPlaneUrl ?? environment.DURABLE_ACTORS_CONTROL_PLANE_URL
    const apiKey = options.apiKey ?? environment.DURABLE_ACTORS_SECRET
    return {
        projectId: options.projectId ?? environment.DURABLE_ACTORS_PROJECT_ID,
        controlPlaneUrl: controlPlaneUrl ?? "http://127.0.0.1:7100",
        apiKey
    }
}

export { SocketProxy }
export type { ProxyActor, SocketAuthorization, SocketGrant, SocketProxyDependencies, SocketProxyOptions }
