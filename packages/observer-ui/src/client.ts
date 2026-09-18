type ActorResidency = "live" | "dormant" | "unknown"

interface ActorInstance {
    actorId: string
    status: ActorResidency
    connections: ActorConnection[]
}

interface ActorConnection {
    id: string
    metadata: unknown
}

interface ActorInventory {
    namespaceId: string
    actors: { actorType: string; live: number; dormant: number; unknown: number; instances: ActorInstance[] }[]
}

interface ObserverClient {
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
    if (!value || typeof value !== "object" || !("namespaceId" in value) || typeof value.namespaceId !== "string" || !("actors" in value) || !Array.isArray(value.actors)) return false
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
        value.connections.every(isActorConnection)
    )
}

function isActorConnection(value: unknown): value is ActorConnection {
    return !!value && typeof value === "object" && "id" in value && typeof value.id === "string" && "metadata" in value
}

export { HttpObserverClient }
export type { ActorConnection, ActorInstance, ActorInventory, ActorResidency, ObserverClient }
