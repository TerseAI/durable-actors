import { createParser } from "eventsource-parser"

type ActorResidency = "live" | "dormant" | "unknown"

interface ActorInstance {
    actorId: string
    status: ActorResidency
    connections: ActorConnection[]
    waiting?: { id: string; operation: string }[] | null
}

interface ActorConnection {
    id: string
    metadata: unknown
}

interface ActorInventory {
    actors: { actorType: string; live: number; dormant: number; unknown: number; instances: ActorInstance[] }[]
}

interface ObserverClient {
    query?(query: ObserverQuery, signal?: AbortSignal): Promise<ObserverQueryResult>
    watchRequests?(onPage: (page: RequestTracePage) => void, signal: AbortSignal, after?: string): Promise<void>
    watchActors?(onInventory: (inventory: ActorInventory) => void, signal: AbortSignal): Promise<void>
    listActors(signal?: AbortSignal): Promise<ActorInventory>
    checkConnection(signal?: AbortSignal): Promise<void>
}

class HttpObserverClient implements ObserverClient {
    constructor(
        private readonly baseUrl: string = "/api/observe",
        private readonly request: typeof fetch = globalThis.fetch.bind(globalThis)
    ) {}

    async listActors(signal?: AbortSignal): Promise<ActorInventory> {
        const result = await this.get("actors", signal)
        if (!isInventory(result)) throw new Error("Invalid actor inventory response.")
        return result
    }

    async watchActors(onInventory: (inventory: ActorInventory) => void, signal: AbortSignal): Promise<void> {
        return this.watch("events", "inventory", isInventory, onInventory, signal)
    }

    async watchRequests(onPage: (page: RequestTracePage) => void, signal: AbortSignal, after?: string): Promise<void> {
        const query = after ? `?${new URLSearchParams({ after })}` : ""
        return this.watch(`requests/events${query}`, "requests", isTracePage, onPage, signal)
    }

    async query(query: ObserverQuery, signal?: AbortSignal): Promise<ObserverQueryResult> {
        const response = await this.request(`${this.baseUrl.replace(/\/$/u, "")}/query`, {
            method: "POST",
            credentials: "same-origin",
            redirect: "error",
            signal,
            headers: { "content-type": "application/json", accept: "application/json" },
            body: JSON.stringify(query)
        })
        if (!response.ok) throw new Error(`Observability query failed (HTTP ${response.status}).`)
        const result: unknown = await response.json()
        if (
            !result ||
            typeof result !== "object" ||
            !("rows" in result) ||
            !Array.isArray(result.rows) ||
            result.rows.length > 500 ||
            !("truncated" in result) ||
            typeof result.truncated !== "boolean" ||
            !result.rows.every(
                row => row && typeof row === "object" && !Array.isArray(row) && Object.values(row).every(value => value === null || ["string", "boolean", "number"].includes(typeof value))
            )
        )
            throw new Error("Invalid SQL query response")
        return result as ObserverQueryResult
    }

    private async watch<T>(path: string, eventName: string, validate: (value: unknown) => value is T, receive: (value: T) => void, signal: AbortSignal): Promise<void> {
        const response = await this.request(`${this.baseUrl.replace(/\/$/u, "")}/${path}`, {
            signal,
            credentials: "same-origin",
            redirect: "error",
            headers: { accept: "text/event-stream" }
        })
        if (!response.ok || !response.headers.get("content-type")?.startsWith("text/event-stream") || !response.body) {
            await response.body?.cancel()
            throw new Error("Live inventory is unavailable")
        }
        const reader = response.body.getReader()
        const decoder = new TextDecoder()
        const parser = createParser({
            onEvent(event) {
                if (signal.aborted) return
                if (event.event === "error") throw new Error("Live inventory is unavailable")
                if (event.event !== eventName) return
                const inventory: unknown = JSON.parse(event.data)
                if (!validate(inventory)) throw new Error("Invalid observer response")
                receive(inventory)
            }
        })
        const cancel = () => {
            void reader.cancel().catch(() => {})
        }
        signal.addEventListener("abort", cancel, { once: true })
        try {
            while (!signal.aborted) {
                const { value, done } = await reader.read()
                if (done) break
                parser.feed(decoder.decode(value, { stream: true }))
            }
            if (!signal.aborted) throw new Error("Live inventory disconnected")
        } finally {
            signal.removeEventListener("abort", cancel)
            await reader.cancel().catch(() => {})
            reader.releaseLock()
        }
    }

    async checkConnection(signal?: AbortSignal): Promise<void> {
        const result = await this.get("connection", signal)
        if (!result || typeof result !== "object" || !("connected" in result) || result.connected !== true) throw new Error("Invalid connection response.")
    }

    private async get(path: string, signal?: AbortSignal): Promise<unknown> {
        const response = await this.request(`${this.baseUrl.replace(/\/$/u, "")}/${path}`, {
            method: "GET",
            credentials: "same-origin",
            redirect: "error",
            signal,
            headers: { accept: "application/json" }
        })
        if (!response.ok) throw new Error(`Connection check failed (HTTP ${response.status}).`)
        return response.json()
    }
}

function isInventory(value: unknown): value is ActorInventory {
    if (!value || typeof value !== "object" || !("actors" in value) || !Array.isArray(value.actors)) return false
    return value.actors.every(
        row =>
            row &&
            typeof row.actorType === "string" &&
            [row.live, row.dormant, row.unknown].every(count => Number.isSafeInteger(count) && count >= 0) &&
            Array.isArray(row.instances) &&
            row.instances.every(isActorInstance)
    )
}

function isActorInstance(value: unknown): value is ActorInstance {
    return (
        !!value &&
        typeof value === "object" &&
        "actorId" in value &&
        typeof value.actorId === "string" &&
        "status" in value &&
        ["live", "dormant", "unknown"].includes(String(value.status)) &&
        "connections" in value &&
        Array.isArray(value.connections) &&
        value.connections.every(isActorConnection) &&
        (!("waiting" in value) ||
            value.waiting == null ||
            (Array.isArray(value.waiting) &&
                value.waiting.every(operation => !!operation && typeof operation === "object" && typeof operation.id === "string" && typeof operation.operation === "string")))
    )
}

function isActorConnection(value: unknown): value is ActorConnection {
    return !!value && typeof value === "object" && "id" in value && typeof value.id === "string" && "metadata" in value
}

export { HttpObserverClient }
export type { ActorConnection, ActorInstance, ActorInventory, ActorResidency, ObserverClient }

export interface RequestTrace {
    eventId?: string
    sequence: number
    requestId: string
    hostId: string
    sessionId: string
    actorType: string
    actorId: string
    kind: "method" | "websocket"
    operation: string
    connectionId: string | null
    startedAtMs: number
    durationMs: number
    queueWaitMs: number | null
    outcome: "completed" | "failed" | "rejected" | "rerouted" | "interrupted"
}

export type SqlValue = string | number | boolean | null
export interface ObserverQuery {
    sql: string
    params: SqlValue[]
}
export interface ObserverQueryResult {
    rows: Record<string, SqlValue>[]
    truncated: boolean
}

export interface RequestTracePage {
    nextCursor?: string | null
    resumeCursor?: string
    reset?: boolean
    epoch: string
    cursor: number
    capacity: number
    evicted: number
    dropped: number
    persistenceFailed?: boolean
    records: RequestTrace[]
}

function isTracePage(value: unknown): value is RequestTracePage {
    if (!value || typeof value !== "object") return false
    const page = value as RequestTracePage
    return (
        typeof page.epoch === "string" &&
        [page.cursor, page.capacity, page.evicted, page.dropped].every(nonnegativeInteger) &&
        (page.persistenceFailed === undefined || typeof page.persistenceFailed === "boolean") &&
        (page.nextCursor == null || typeof page.nextCursor === "string") &&
        (page.resumeCursor === undefined || typeof page.resumeCursor === "string") &&
        (page.reset === undefined || typeof page.reset === "boolean") &&
        page.capacity > 0 &&
        page.capacity <= 500 &&
        Array.isArray(page.records) &&
        page.records.length <= page.capacity &&
        page.records.every(record => isTrace(record) && record.sequence <= page.cursor)
    )
}

function nonnegativeInteger(value: unknown): value is number {
    return Number.isSafeInteger(value) && Number(value) >= 0
}

export function isTrace(value: unknown): value is RequestTrace {
    if (!value || typeof value !== "object") return false
    const trace = value as RequestTrace
    return (
        [trace.requestId, trace.hostId, trace.sessionId, trace.actorType, trace.actorId, trace.operation].every(value => typeof value === "string") &&
        (trace.eventId === undefined || (typeof trace.eventId === "string" && trace.eventId.length > 0)) &&
        nonnegativeInteger(trace.sequence) &&
        nonnegativeInteger(trace.startedAtMs) &&
        trace.startedAtMs <= 8.64e15 &&
        ["method", "websocket"].includes(trace.kind) &&
        ["completed", "failed", "rejected", "rerouted", "interrupted"].includes(trace.outcome) &&
        (trace.connectionId === null || typeof trace.connectionId === "string") &&
        Number.isFinite(trace.durationMs) &&
        trace.durationMs >= 0 &&
        (trace.queueWaitMs === null || (Number.isFinite(trace.queueWaitMs) && trace.queueWaitMs >= 0 && trace.queueWaitMs <= trace.durationMs))
    )
}
