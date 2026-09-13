import { decodeFrame, decodeGrant, encodeFrame } from "./protocol.js"
import { StateCache } from "./state.js"
import { SocketError } from "./types.js"
import type {
    ActorDescriptor,
    ActorTarget,
    BrowserSocket,
    ClientDependencies,
    ClientOptions,
    ConnectionEvents,
    ConnectionStatus
} from "./types.js"

class ActorConnection<Incoming, Outgoing, State extends object, Field extends keyof State> {
    private readonly runtime: Required<ClientDependencies>
    private readonly cache: StateCache<State>
    private readonly listeners = new Map<string, Set<(value: any) => void>>()
    private readonly subscribers = new Map<Field, Set<(value: any) => void>>()
    private socket: BrowserSocket | undefined
    private request: AbortController | undefined
    private timer: unknown
    private requestTimer: unknown
    private generation = 0
    private active = false
    private attempt = 0
    private connectionId: string | undefined
    private snapshotReceived = false
    private renewalPending = false
    private authorizationDeadline = 0
    private waiting: { promise: Promise<void>; resolve: () => void; reject: (error: Error) => void } | undefined
    private statusValue: ConnectionStatus = "idle"

    constructor(
        private readonly target: ActorTarget,
        private readonly descriptor: ActorDescriptor<Incoming, Outgoing, State, Field>,
        private readonly options: ClientOptions,
        dependencies: ClientDependencies = {}
    ) {
        this.runtime = {
            fetch: dependencies.fetch ?? options.fetch ?? globalThis.fetch.bind(globalThis),
            connectWebSocket: dependencies.connectWebSocket ?? (url => new WebSocket(url, "little-actors.v1")),
            now: dependencies.now ?? (() => performance.now()),
            random: dependencies.random ?? Math.random,
            schedule: dependencies.schedule ?? ((callback, delay) => setTimeout(callback, delay)),
            cancel: dependencies.cancel ?? (timer => clearTimeout(timer as ReturnType<typeof setTimeout>))
        }
        this.cache = new StateCache<State>()
    }

    get status(): ConnectionStatus {
        return this.statusValue
    }
    get state(): Readonly<State> | undefined {
        return this.cache.current
    }

    connect(): Promise<void> {
        if (this.status === "open") return Promise.resolve()
        if (!this.waiting) {
            let resolve!: () => void
            let reject!: (error: Error) => void
            const promise = new Promise<void>((done, fail) => {
                resolve = done
                reject = fail
            })
            this.waiting = { promise, resolve, reject }
        }
        const promise = this.waiting.promise
        if (!this.active) {
            this.active = true
            this.attempt = 0
            this.generation++
            this.setStatus("connecting")
            void this.open(this.generation)
        }
        return promise
    }

    send(message: Incoming): void {
        if (this.status !== "open" || this.socket?.readyState !== 1 || this.runtime.now() >= this.authorizationDeadline)
            throw new SocketError("not_open", "Actor connection is not open")
        this.socket.send(encodeFrame({ type: "message", data: message }))
    }

    close(): void {
        this.stop("closed", new SocketError("closed", "Actor connection was closed"))
    }

    on<Event extends keyof ConnectionEvents<Outgoing>>(
        event: Event,
        listener: (value: ConnectionEvents<Outgoing>[Event]) => void
    ): () => void {
        const listeners = this.listeners.get(event) ?? new Set()
        listeners.add(listener)
        this.listeners.set(event, listeners)
        return () => {
            listeners.delete(listener)
        }
    }

    subscribe<Key extends Field>(field: Key, listener: (value: State[Key]) => void): () => void {
        if (!this.descriptor.emittable.includes(field))
            throw new SocketError("invalid_field", `${String(field)} is not an @Emittable field`)
        const listeners = this.subscribers.get(field) ?? new Set()
        listeners.add(listener)
        this.subscribers.set(field, listeners)
        if (this.cache.current !== undefined) this.notify(listener, this.cache.field(field))
        return () => {
            listeners.delete(listener)
        }
    }

    private async open(generation: number): Promise<void> {
        if (!this.current(generation)) return
        try {
            const grant = await this.grant()
            if (!this.current(generation)) return
            const socket = this.runtime.connectWebSocket(grant.websocketUrl)
            this.socket = socket
            this.connectionId = undefined
            this.snapshotReceived = false
            this.schedule(
                () => this.lost(new SocketError("timeout", "WebSocket initialization timed out", true)),
                10000
            )
            const current = () => this.current(generation) && this.socket === socket
            socket.addEventListener("open", () => {
                if (!current()) return
                try {
                    socket.send(encodeFrame({ type: "authorize", key: grant.key }))
                } catch (error) {
                    this.lost(socketError(error))
                }
            })
            socket.addEventListener("message", event => {
                if (!current()) return
                try {
                    this.receive((event as MessageEvent).data)
                } catch (error) {
                    this.stop("error", socketError(error))
                }
            })
            socket.addEventListener("error", () => {
                if (current()) this.lost(new SocketError("network", "WebSocket transport failed", true))
            })
            socket.addEventListener("close", event => {
                if (!current()) return
                const { code, reason } = event as CloseEvent
                this.emit("close", { code, reason })
                if (!current()) return
                const retryable = [1001, 1006, 1011, 1012, 1013, 4408, 4409].includes(code)
                const error = new SocketError(`socket_${code}`, reason || `WebSocket closed (${code})`, retryable)
                if (retryable) this.lost(error)
                else this.stop(code === 1000 ? "closed" : "error", error)
            })
        } catch (error) {
            if (this.current(generation)) this.lost(socketError(error))
        }
    }

    private receive(data: unknown): void {
        const frame = decodeFrame(data)
        switch (frame.type) {
            case "state":
                if (this.snapshotReceived) throw new SocketError("invalid_protocol", "Duplicate initial state")
                this.cache.snapshot(frame.state, frame.version)
                this.snapshotReceived = true
                this.notifyFields(this.descriptor.emittable)
                return
            case "state_update":
                this.notifyFields(this.cache.update(frame.changes, frame.removed, frame.version))
                return
            case "message":
                this.emit("message", frame.data as Outgoing)
                return
            case "ready":
                if (!this.snapshotReceived || this.connectionId)
                    throw new SocketError("invalid_protocol", "Invalid socket readiness sequence")
                this.connectionId = frame.connectionId
                this.attempt = 0
                this.authorized(frame.expiresInMs)
                this.setStatus("open")
                this.waiting?.resolve()
                this.waiting = undefined
                this.emit("open", undefined)
                return
            case "renewed":
                if (!this.renewalPending) throw new SocketError("invalid_protocol", "Unexpected socket renewal")
                this.renewalPending = false
                this.authorized(frame.expiresInMs)
        }
    }

    private authorized(lifetime: number): void {
        this.authorizationDeadline = this.runtime.now() + lifetime
        this.schedule(() => {
            void this.renew(this.generation)
        }, lifetime * 0.8)
    }

    private async renew(generation: number): Promise<void> {
        if (!this.current(generation) || !this.connectionId) return
        const remaining = this.authorizationDeadline - this.runtime.now()
        const expired = () => this.lost(new SocketError("expired", "Socket authorization expired", true))
        if (remaining <= 0) {
            expired()
            return
        }
        this.schedule(expired, remaining)
        try {
            const grant = await this.grant()
            if (!this.current(generation) || this.status !== "open") return
            this.renewalPending = true
            this.socket!.send(encodeFrame({ type: "renew", key: grant.key }))
            this.schedule(
                () => this.lost(new SocketError("timeout", "Socket authorization renewal timed out", true)),
                Math.min(10000, Math.max(0, this.authorizationDeadline - this.runtime.now()))
            )
        } catch (error) {
            if (!this.current(generation)) return
            const failure = socketError(error)
            if (!failure.retryable) {
                this.stop("error", failure)
                return
            }
            if (this.runtime.now() >= this.authorizationDeadline) {
                this.lost(failure)
                return
            }
            this.emit("error", failure)
            this.schedule(
                () => {
                    void this.renew(generation)
                },
                Math.min(this.backoff(), Math.max(0, this.authorizationDeadline - this.runtime.now()))
            )
        }
    }

    private async grant() {
        const controller = new AbortController()
        this.request = controller
        const timer = this.runtime.schedule(() => controller.abort(), 10000)
        this.requestTimer = timer
        try {
            const endpoint =
                (typeof this.options.endpoint === "function"
                    ? this.options.endpoint(this.target)
                    : this.options.endpoint) ??
                `/api/socket/${encodeURIComponent(this.target.actorType)}/${encodeURIComponent(this.target.actorId)}`
            const response = await this.runtime.fetch(endpoint, {
                method: "POST",
                cache: "no-store",
                signal: controller.signal
            })
            if (!response.ok)
                throw new SocketError(
                    `http_${response.status}`,
                    `Socket proxy returned HTTP ${response.status}`,
                    response.status === 429 || response.status >= 500
                )
            try {
                return decodeGrant(await response.json())
            } catch {
                throw new SocketError("invalid_authorization", "Invalid response from the socket proxy")
            }
        } finally {
            this.runtime.cancel(timer)
            if (this.request === controller) this.request = undefined
        }
    }

    private lost(error: SocketError): void {
        if (!this.active) return
        if (!error.retryable) {
            this.stop("error", error)
            return
        }
        this.disconnect()
        this.generation++
        const generation = this.generation
        this.setStatus("reconnecting")
        if (!this.current(generation)) return
        this.emit("error", error)
        if (!this.current(generation)) return
        this.schedule(() => {
            void this.open(this.generation)
        }, this.backoff())
    }

    private stop(status: "closed" | "error", error: SocketError): void {
        this.active = false
        this.generation++
        this.disconnect()
        this.setStatus(status)
        this.waiting?.reject(error)
        this.waiting = undefined
        if (status === "error") this.emit("error", error)
    }

    private disconnect(): void {
        this.renewalPending = false
        this.runtime.cancel(this.timer)
        this.runtime.cancel(this.requestTimer)
        this.request?.abort()
        this.request = undefined
        const socket = this.socket
        this.socket = undefined
        if (socket && socket.readyState < 2) socket.close(1000, "closed")
    }

    private current(generation: number): boolean {
        return this.active && generation === this.generation
    }
    private backoff(): number {
        return Math.min(30000, 500 * 2 ** Math.min(this.attempt++, 6)) * (0.5 + this.runtime.random() * 0.5)
    }
    private schedule(callback: () => void, delay: number): void {
        this.runtime.cancel(this.timer)
        this.timer = this.runtime.schedule(callback, delay)
    }
    private setStatus(status: ConnectionStatus): void {
        this.statusValue = status
        this.emit("status", status)
    }
    private emit<Event extends keyof ConnectionEvents<Outgoing>>(
        event: Event,
        value: ConnectionEvents<Outgoing>[Event]
    ): void {
        for (const listener of this.listeners.get(event) ?? []) this.notify(listener, value)
    }
    private notifyFields(fields: readonly PropertyKey[]): void {
        for (const field of fields)
            for (const listener of this.subscribers.get(field as Field) ?? [])
                this.notify(listener, this.cache.field(field as Field))
    }
    private notify(listener: (value: any) => void, value: unknown): void {
        try {
            listener(value)
        } catch (error) {
            queueMicrotask(() => {
                throw error
            })
        }
    }
}

function socketError(error: unknown): SocketError {
    return error instanceof SocketError
        ? error
        : new SocketError("network", error instanceof Error ? error.message : "Socket request failed", true)
}

export { ActorConnection }
