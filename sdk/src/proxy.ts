/** @module durable-actors/proxy */
import { z } from "zod"

import { projectActorPath, validateActorComponent, validateProjectId } from "./actor/identity.js"
import { socketMetadata } from "./actor/socketValidation.js"
import { actorEnvironment } from "./environment.js"
import type {
    ProxyActor,
    SocketAuthorization,
    SocketGrant,
    SocketProxyDependencies,
    SocketProxyOptions
} from "./generated-runtime/types.js"

const socketGrantSchema = z.object({
    websocketUrl: z.url().refine(url => ["ws:", "wss:"].includes(new URL(url).protocol)),
    homeRegion: z.string().min(1),
    connectByMs: z.number().int(),
    authorizedUntilMs: z.number().int()
})

/** Issues browser connection URLs from your backend. */
class SocketProxy<Actors extends Record<string, ProxyActor>> {
    private readonly projectId: string
    private readonly origin: string
    private readonly apiKey: string
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
        const settings = proxySettings(options)
        const url = new URL(settings.controlPlaneUrl)
        if (
            !["https:", "http:"].includes(url.protocol) ||
            url.pathname !== "/" ||
            url.search ||
            url.hash ||
            url.username ||
            url.password
        )
            throw new Error("Control-plane URL must be an HTTP(S) origin")
        this.origin = url.origin
        this.apiKey = settings.apiKey ?? ""
        if (typeof this.apiKey !== "string" || !this.apiKey || this.apiKey.trim() !== this.apiKey)
            throw new Error("A backend shared secret is required; set DURABLE_ACTORS_SECRET or pass apiKey")
        this.projectId = validateProjectId(settings.projectId)
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
                headers: { authorization: `Bearer ${this.apiKey}`, "content-type": "application/json" },
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
