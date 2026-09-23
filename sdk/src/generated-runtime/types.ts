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

interface ActorRpcTransport {
    invoke(actorName: string, actorId: string, method: string, args: readonly unknown[]): Promise<unknown>
}

interface ActorRpcMethod {
    readonly name: string
    readonly result: "void" | "value"
}

/** Browser connection URL and deadlines in Unix milliseconds. Treat the URL as a credential. */
interface SocketGrant {
    websocketUrl: string
    homeRegion: string
    connectByMs: number
    authorizedUntilMs: number
}

export type {
    ActorRpcTransport,
    ActorRpcMethod,
    ProxyActor,
    SocketAuthorization,
    SocketGrant,
    SocketProxyDependencies,
    SocketProxyOptions
}
