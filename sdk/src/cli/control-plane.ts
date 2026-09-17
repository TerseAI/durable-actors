import { configuredSettings } from "../client/clientSettings.js"
import { readLocalSettings } from "../client/localSettings.js"

interface ControlPlaneOptions {
    url?: string | true
    apiKey?: string
    namespace?: string
}

interface ControlPlaneConnection {
    controlPlaneUrl: string
    credential: string
    namespaceId?: string
}

class ControlPlaneClient {
    constructor(
        readonly connection: ControlPlaneConnection,
        private readonly request: typeof fetch
    ) {}

    registerDeployment(deployment: unknown): Promise<unknown> {
        return this.requestJson("PUT", this.namespacePath("deployment"), deployment)
    }

    getContract(revision?: string): Promise<unknown> {
        const query = revision ? `?${new URLSearchParams({ revision })}` : ""
        return this.requestJson("GET", this.namespacePath(`contract${query}`))
    }

    listObjects(query: URLSearchParams): Promise<unknown> {
        return this.requestJson("GET", `/v1/objects${query.size ? `?${query}` : ""}`)
    }

    inspectObject(actorType: string, actorId: string): Promise<unknown> {
        const namespace = encodeURIComponent(this.connection.namespaceId ?? "default")
        return this.requestJson(
            "GET",
            `/v1/namespaces/${namespace}/actors/${encodeURIComponent(actorType)}/${encodeURIComponent(actorId)}/state`
        )
    }

    issueSessionToken(request: {
        executionId: string
        deadlineUnixMs: number
        storageRegion: string
    }): Promise<unknown> {
        return this.requestJson("POST", this.namespacePath("session-scoped-token"), request, 10_000)
    }

    private namespacePath(resource: string): string {
        const namespace = this.connection.namespaceId
        const prefix = namespace ? `/v1/namespaces/${encodeURIComponent(namespace)}` : "/v1"
        return `${prefix}/${resource}`
    }

    private async requestJson(
        method: "GET" | "PUT" | "POST",
        pathname: string,
        body?: unknown,
        timeoutMs = 30_000
    ): Promise<unknown> {
        const { controlPlaneUrl, credential } = this.connection
        const response = await this.request(`${controlPlaneUrl}${pathname}`, {
            method,
            headers: {
                authorization: `Bearer ${credential}`,
                ...(body === undefined ? {} : { "content-type": "application/json" })
            },
            body: body === undefined ? undefined : JSON.stringify(body),
            signal: AbortSignal.timeout(timeoutMs),
            redirect: "error"
        }).catch(() => {
            throw new Error(
                `Cannot complete ${method} ${pathname} at ${controlPlaneUrl}. Check the URL and runtime.${method === "GET" ? "" : " The request may have reached the server."}`
            )
        })
        return readResponse(response)
    }
}

function createControlPlaneClient(
    options: ControlPlaneOptions,
    request: typeof fetch,
    readLocal: typeof readLocalSettings = readLocalSettings
): ControlPlaneClient {
    const url = typeof options.url === "string" ? options.url : process.env.DURABLE_OBJECT_CONTROL_PLANE_URL
    const key = options.apiKey || process.env.DURABLE_OBJECT_API_KEY
    const local = !url && !key ? readLocal() : {}
    const controlPlaneUrl = url || local.controlPlaneUrl
    const apiKey = key || local.apiKey
    if (!controlPlaneUrl)
        throw new Error("Start `npx little-actors dev` first, or set --url or DURABLE_OBJECT_CONTROL_PLANE_URL.")
    if (!apiKey) throw new Error("An admin API key is required. Set DURABLE_OBJECT_API_KEY or --api-key.")
    const connection = configuredSettings({
        controlPlaneUrl,
        apiKey,
        namespaceId: options.namespace || process.env.DURABLE_OBJECT_NAMESPACE_ID || local.namespaceId
    })
    return new ControlPlaneClient(connection, request)
}

async function readResponse(response: Response): Promise<unknown> {
    const result: unknown = await response.json().catch(() => {
        if (response.ok) throw new Error(`Control plane returned invalid JSON (HTTP ${response.status}).`)
        return undefined
    })
    if (!response.ok)
        throw new Error(
            `Control-plane request failed (HTTP ${response.status}): ${errorMessage(result) ?? response.statusText}`
        )
    return result
}

function errorMessage(result: unknown): string | undefined {
    if (!result || typeof result !== "object" || !("error" in result)) return undefined
    const error = result.error
    if (!error || typeof error !== "object" || !("message" in error)) return undefined
    return typeof error.message === "string" ? error.message : undefined
}

export { ControlPlaneClient, createControlPlaneClient }
export type { ControlPlaneConnection, ControlPlaneOptions }
