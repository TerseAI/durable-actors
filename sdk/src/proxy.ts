import { z } from "zod"

import { validateActorComponent } from "./actor/identity.js"
import { socketMetadata } from "./actor/socketValidation.js"

interface SocketProxyOptions {
    readonly controlPlaneUrl?: string
    readonly apiKey?: string
    readonly namespaceId?: string
}

interface SocketProxyDependencies {
    readonly fetch?: typeof globalThis.fetch
}

interface ProxyActor<Metadata = unknown> {
    readonly types?: Metadata
}

type SocketAuthorization<Actors extends Record<string, ProxyActor>> = {
    [Name in keyof Actors & string]: {
        readonly actorType: Name
        readonly actorId: string
        readonly metadata: Actors[Name] extends ProxyActor<infer Metadata> ? Metadata : never
        readonly authorizationLifetimeMs?: number
    }
}[keyof Actors & string]

const socketGrantSchema = z.object({
    websocketUrl: z.url().refine(url => ["ws:", "wss:"].includes(new URL(url).protocol)),
    key: z.string().min(1)
})

type SocketGrant = z.infer<typeof socketGrantSchema>

class SocketProxy<Actors extends Record<string, ProxyActor>> {
    private readonly origin: string
    private readonly apiKey: string
    private readonly namespace: string | undefined
    private readonly fetchRequest: typeof globalThis.fetch

    constructor(
        private readonly actors: Actors,
        options: SocketProxyOptions = {},
        dependencies: SocketProxyDependencies = {}
    ) {
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
            throw new Error("A backend API key is required; set DURABLE_OBJECT_API_KEY or pass apiKey")
        this.namespace = settings.namespaceId
        if (this.namespace) validateActorComponent("namespace ID", this.namespace)
        this.fetchRequest = dependencies.fetch ?? globalThis.fetch
    }

    async handle(authorization: SocketAuthorization<Actors>): Promise<SocketGrant> {
        const actorType = validateActorComponent("actor type", authorization.actorType)
        const actorId = validateActorComponent("actor ID", authorization.actorId)
        if (!Object.hasOwn(this.actors, actorType)) throw new Error(`Unknown actor type: ${actorType}`)
        const metadata = socketMetadata(authorization.metadata)
        const authorizationLifetimeMs = authorization.authorizationLifetimeMs ?? 900000
        if (
            !Number.isSafeInteger(authorizationLifetimeMs) ||
            authorizationLifetimeMs < 1000 ||
            authorizationLifetimeMs > 86400000
        )
            throw new Error("Socket authorization lifetime must be between one second and one day")
        const scope = this.namespace ? `/namespaces/${encodeURIComponent(this.namespace)}` : ""
        const response = await this.fetchRequest(
            `${this.origin}/v1${scope}/actors/${encodeURIComponent(actorType)}/${encodeURIComponent(actorId)}/socket-ticket`,
            {
                method: "POST",
                headers: { authorization: `Bearer ${this.apiKey}`, "content-type": "application/json" },
                body: JSON.stringify({ metadata, authorizationLifetimeMs }),
                signal: AbortSignal.timeout(10000)
            }
        )
        if (!response.ok) throw new Error(`WebSocket authorization could not be issued (HTTP ${response.status})`)
        return socketGrantSchema.parse(await response.json())
    }
}

function proxySettings(options: SocketProxyOptions) {
    const controlPlaneUrl = options.controlPlaneUrl ?? process.env.DURABLE_OBJECT_CONTROL_PLANE_URL
    const apiKey = options.apiKey ?? process.env.DURABLE_OBJECT_API_KEY
    return {
        controlPlaneUrl: controlPlaneUrl ?? "http://127.0.0.1:7100",
        apiKey,
        namespaceId: options.namespaceId ?? process.env.DURABLE_OBJECT_NAMESPACE_ID
    }
}

export { SocketProxy }
export type { ProxyActor, SocketAuthorization, SocketGrant, SocketProxyDependencies, SocketProxyOptions }
