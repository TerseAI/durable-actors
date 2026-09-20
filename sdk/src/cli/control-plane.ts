import { projectActorPath, validateProjectId } from "../actor/identity.js"

import { connection } from "./connection.js"

interface ControlPlaneOptions {
    projectId: string
    url?: string | true
    apiKey?: string
}

interface ControlPlaneConnection {
    projectId: string
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
        return this.requestJson("PUT", `${this.projectPath()}/deployment`, deployment)
    }

    getContract(revision?: string): Promise<unknown> {
        const query = revision ? `?${new URLSearchParams({ revision })}` : ""
        return this.requestJson("GET", `${this.projectPath()}/deployment/contract${query}`)
    }

    listActors(): Promise<unknown> {
        return this.requestJson("GET", "/v1/observe/actors")
    }

    async openActorStream(signal: AbortSignal): Promise<Response> {
        return this.openStream("/v1/observe/events", signal)
    }

    async openRequestStream(signal: AbortSignal, after?: string): Promise<Response> {
        const query = after ? `?${new URLSearchParams({ after })}` : ""
        return this.openStream(`/v1/observe/requests/events${query}`, signal)
    }

    query(query: { sql: string; params?: unknown[] }, signal?: AbortSignal): Promise<unknown> {
        return this.requestJson("POST", "/v1/observe/query", query, 30_000, signal)
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

    inspectObject(actorName: string, actorId: string): Promise<unknown> {
        return this.requestJson(
            "GET",
            `${projectActorPath(this.connection.projectId, actorName, actorId)}?include=state`
        )
    }

    private projectPath(): string {
        return `/v1/projects/${encodeURIComponent(validateProjectId(this.connection.projectId))}`
    }

    private async requestJson(
        method: "GET" | "PUT" | "POST",
        pathname: string,
        body?: unknown,
        timeoutMs = 30_000,
        signal?: AbortSignal
    ): Promise<unknown> {
        const { controlPlaneUrl, credential } = this.connection
        const response = await this.request(`${controlPlaneUrl}${pathname}`, {
            method,
            headers: {
                authorization: `Bearer ${credential}`,
                ...(body === undefined ? {} : { "content-type": "application/json" })
            },
            body: body === undefined ? undefined : JSON.stringify(body),
            signal: signal ? AbortSignal.any([signal, AbortSignal.timeout(timeoutMs)]) : AbortSignal.timeout(timeoutMs),
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
            projectId: options.projectId,
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
