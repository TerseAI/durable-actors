import { ActorProtocolError } from "./errors.js"
import { isRecord } from "./http.js"
import type { SocketGrant } from "./proxy.js"

/** Subscribe using a backend-authorized grant, renewed for every connection attempt. */
export function subscribe<State extends object>(
    getGrant: () => Promise<Pick<SocketGrant, "websocketUrl">>,
    onState: (state: State) => void,
    options: SubscriptionOptions = {}
): Subscription<State> {
    return new Subscription(getGrant, onState, options)
}

/** Receives complete emitted state. Call close() when the consumer is disposed. */
export class Subscription<State extends object> {
    private socket: SubscriptionSocket | undefined
    private detach: (() => void) | undefined
    private timer: ReturnType<typeof setTimeout> | undefined
    private retryDelay = 1000
    private stopping = false
    private failure: Error | undefined
    private readonly connectWebSocket: (url: string) => SubscriptionSocket

    constructor(
        private readonly getGrant: () => Promise<Pick<SocketGrant, "websocketUrl">>,
        private readonly onState: (state: State) => void,
        private readonly options: SubscriptionOptions = {},
        dependencies: SubscriptionDependencies = {}
    ) {
        this.connectWebSocket = dependencies.connectWebSocket ?? (url => new WebSocket(url))
        queueMicrotask(() => void this.connect())
    }

    get closed(): boolean {
        return this.stopping
    }

    /** Terminal protocol or callback failure; connection failures are retried. */
    get error(): Error | undefined {
        return this.failure
    }

    close(): void {
        this.stopping = true
        clearTimeout(this.timer)
        this.disconnect()
    }

    private async connect(): Promise<void> {
        if (this.stopping) return
        try {
            const grant = await this.getGrant()
            if (this.stopping) return
            if (!URL.canParse(grant.websocketUrl) || !/^wss?:$/u.test(new URL(grant.websocketUrl).protocol))
                throw new ActorProtocolError("grant must contain a ws: or wss: websocketUrl")
            const socket = this.connectWebSocket(grant.websocketUrl)
            this.socket = socket
            const state = new StateAccumulator<State>()
            const message: EventListener = event => {
                if (this.socket !== socket || this.stopping) return
                try {
                    const data: unknown = (event as MessageEvent).data
                    if (typeof data !== "string") throw new ActorProtocolError("actor state frames must be text")
                    const next = state.apply(JSON.parse(data))
                    if (next !== undefined) {
                        this.retryDelay = 1000
                        this.onState(next)
                    }
                } catch (error) {
                    this.stop(error)
                }
            }
            const failed = () => {
                if (this.socket === socket) this.retry(new Error("actor WebSocket disconnected"))
            }
            socket.addEventListener("message", message)
            socket.addEventListener("error", failed)
            socket.addEventListener("close", failed)
            this.detach = () => {
                socket.removeEventListener("message", message)
                socket.removeEventListener("error", failed)
                socket.removeEventListener("close", failed)
            }
        } catch (error) {
            if (this.stopping) return
            if (error instanceof ActorProtocolError) this.stop(error)
            else this.retry(error)
        }
    }

    private retry(error: unknown): void {
        this.disconnect()
        this.report(error)
        if (this.stopping) return
        this.timer = setTimeout(() => void this.connect(), this.retryDelay)
        this.retryDelay = Math.min(this.retryDelay * 2, 30000)
    }

    private stop(error: unknown): void {
        this.failure = asError(error)
        this.close()
        this.report(this.failure)
    }

    private disconnect(): void {
        this.detach?.()
        this.detach = undefined
        const socket = this.socket
        this.socket = undefined
        socket?.close()
    }

    private report(error: unknown): void {
        try {
            if (this.options.onError) this.options.onError(asError(error))
            else console.error(error)
        } catch (handlerError) {
            console.error(handlerError)
        }
    }
}

export interface SubscriptionOptions {
    /** Reports retryable connection errors and terminal protocol/callback errors. */
    readonly onError?: (error: Error) => void
}

export interface SubscriptionSocket {
    addEventListener(type: string, listener: EventListener): void
    removeEventListener(type: string, listener: EventListener): void
    close(): void
}

export interface SubscriptionDependencies {
    readonly connectWebSocket?: (url: string) => SubscriptionSocket
}

class StateAccumulator<State extends object> {
    private value: Record<string, unknown> | undefined
    private version = -1

    apply(frame: unknown): State | undefined {
        if (!isRecord(frame) || (frame.type !== "state" && frame.type !== "state_update")) return
        const version = frame.version === undefined ? 0 : frame.version
        if (!Number.isSafeInteger(version) || (version as number) < 0)
            throw new ActorProtocolError("actor state version must be a nonnegative safe integer")
        if (frame.type === "state") {
            if (!isRecord(frame.state)) throw new ActorProtocolError("actor state must be an object")
        } else if (
            !isRecord(frame.changes) ||
            !Array.isArray(frame.removed) ||
            !frame.removed.every(name => typeof name === "string") ||
            frame.version === undefined
        ) {
            throw new ActorProtocolError("invalid actor state update")
        }
        if ((version as number) <= this.version) return
        if (frame.type === "state") this.value = { ...(frame.state as Record<string, unknown>) }
        else {
            if (!this.value) throw new ActorProtocolError("state update arrived before the initial snapshot")
            this.value = { ...this.value, ...(frame.changes as Record<string, unknown>) }
            for (const name of frame.removed as string[]) delete this.value[name]
        }
        this.version = version as number
        return structuredClone(this.value) as State
    }
}

function asError(error: unknown): Error {
    return error instanceof Error ? error : new Error(String(error))
}

export { ActorProtocolError } from "./errors.js"
export type { SocketGrant } from "./proxy.js"
export type { ActorStateMessage, ActorStateUpdate } from "./state.js"
