interface ActorDescriptor<
    Incoming = unknown,
    Outgoing = unknown,
    State extends object = object,
    Field extends keyof State = keyof State
> {
    readonly actorType: string
    readonly emittable: readonly Field[]
    readonly types?: { incoming: Incoming; outgoing: Outgoing; state: State; field: Field }
}

type ConnectionStatus = "idle" | "connecting" | "open" | "reconnecting" | "closed" | "error"
interface ActorTarget {
    readonly actorType: string
    readonly actorId: string
}

interface ClientOptions {
    readonly endpoint?: string | ((actor: ActorTarget) => string)
    readonly fetch?: typeof globalThis.fetch
}

interface BrowserSocket extends Pick<EventTarget, "addEventListener"> {
    readonly readyState: number
    send(data: string): void
    close(code?: number, reason?: string): void
}

interface ClientDependencies {
    readonly fetch?: typeof globalThis.fetch
    readonly connectWebSocket?: (url: string) => BrowserSocket
    readonly now?: () => number
    readonly random?: () => number
    readonly schedule?: (callback: () => void, delayMs: number) => unknown
    readonly cancel?: (timer: unknown) => void
}

class SocketError extends Error {
    constructor(
        readonly code: string,
        message: string,
        readonly retryable = false
    ) {
        super(message)
        this.name = "SocketError"
    }
}

interface ConnectionEvents<Outgoing> {
    readonly message: Outgoing
    readonly status: ConnectionStatus
    readonly open: undefined
    readonly close: { readonly code: number; readonly reason: string }
    readonly error: SocketError
}

export { SocketError }
export type {
    ActorDescriptor,
    ActorTarget,
    BrowserSocket,
    ClientDependencies,
    ClientOptions,
    ConnectionEvents,
    ConnectionStatus
}
