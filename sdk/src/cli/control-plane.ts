import { connection } from "./connection.js"

interface ControlPlaneOptions {
    url?: string | true
    apiKey?: string
}

interface ControlPlaneConnection {
    controlPlaneUrl: string
    credential: string
}

class ControlPlaneClient {
    constructor(
        readonly connection: ControlPlaneConnection,
        private readonly request: typeof fetch
    ) {}

    async checkConnection(): Promise<void> {
        await this.requestJson("GET", "/v1/actors?limit=1", undefined, 10_000)
    }

    registerDeployment(deployment: unknown): Promise<unknown> {
        return this.requestJson("PUT", "/v1/deployment", deployment)
    }

    getContract(revision?: string): Promise<unknown> {
        const query = revision ? `?${new URLSearchParams({ revision })}` : ""
        return this.requestJson("GET", `/v1/deployment/contract${query}`)
    }

    listActors(): Promise<unknown> {
        return this.requestJson("GET", "/v1/observe/actors")
    }

    async openActorStream(signal: AbortSignal): Promise<Response> {
        return this.openStream("/v1/observe/events", signal)
    }

    async openRequestStream(signal: AbortSignal): Promise<Response> {
        return this.openStream("/v1/observe/requests/events", signal)
    }

    private async openStream(path: string, signal: AbortSignal): Promise<Response> {
        const response = await this.request(`${this.connection.controlPlaneUrl}${path}`, {
            signal,
            redirect: "error",
            headers: { authorization: `Bearer ${this.connection.credential}`, accept: "text/event-stream" }
        })
        if (!response.ok || !response.headers.get("content-type")?.startsWith("text/event-stream") || !response.body) {
            await response.body?.cancel()
            throw new Error("Live inventory is unavailable")
        }
        return response
    }

    listObjects(query: URLSearchParams): Promise<unknown> {
        return this.requestJson("GET", `/v1/actors${query.size ? `?${query}` : ""}`)
    }

    inspectObject(actorType: string, actorId: string): Promise<unknown> {
        return this.requestJson(
            "GET",
            `/v1/actors/${encodeURIComponent(actorType)}/${encodeURIComponent(actorId)}?include=state`
        )
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

function createControlPlaneClient(options: ControlPlaneOptions, request: typeof fetch): ControlPlaneClient {
    return new ControlPlaneClient(
        connection({
            url:
                typeof options.url === "string"
                    ? options.url
                    : process.env.DURABLE_OBJECT_CONTROL_PLANE_URL || "http://127.0.0.1:7100",
            apiKey: options.apiKey || process.env.DURABLE_OBJECT_API_KEY
        }),
        request
    )
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
