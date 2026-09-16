import { configuredSettings } from "../client/clientSettings.js"

interface ControlPlaneOptions {
    url?: string | true
    apiKey?: string
    namespace?: string
}

class ControlPlaneClient {
    readonly connection: ReturnType<typeof configuredSettings>

    constructor(
        options: ControlPlaneOptions,
        private readonly request: typeof fetch
    ) {
        const controlPlaneUrl =
            typeof options.url === "string" ? options.url : process.env.DURABLE_OBJECT_CONTROL_PLANE_URL
        const apiKey = options.apiKey || process.env.DURABLE_OBJECT_API_KEY
        if (!controlPlaneUrl) throw new Error("Set --url or DURABLE_OBJECT_CONTROL_PLANE_URL.")
        if (!apiKey) throw new Error("An admin API key is required. Set DURABLE_OBJECT_API_KEY or --api-key.")
        this.connection = configuredSettings({
            controlPlaneUrl,
            apiKey,
            namespaceId: options.namespace || process.env.DURABLE_OBJECT_NAMESPACE_ID || undefined
        })
    }

    async json(method: "GET" | "PUT", resource: string, body?: unknown): Promise<unknown> {
        const { controlPlaneUrl, namespaceId, credential } = this.connection
        const prefix = namespaceId ? `/v1/namespaces/${encodeURIComponent(namespaceId)}` : "/v1"
        const response = await this.request(`${controlPlaneUrl}${prefix}/${resource}`, {
            method,
            headers: {
                authorization: `Bearer ${credential}`,
                ...(body === undefined ? {} : { "content-type": "application/json" })
            },
            body: body === undefined ? undefined : JSON.stringify(body),
            signal: AbortSignal.timeout(30_000),
            redirect: "error"
        }).catch(() => {
            throw new Error(
                `Cannot complete ${method} ${prefix}/${resource} at ${controlPlaneUrl}. Check the URL and runtime.${method === "PUT" ? " The deployment request may have reached the server." : ""}`
            )
        })
        const result = await response.json().catch(() => {
            throw new Error(`Control plane returned invalid JSON (HTTP ${response.status}).`)
        })
        if (!response.ok)
            throw new Error(
                `Control-plane request failed (HTTP ${response.status}): ${result?.error?.message ?? response.statusText}`
            )
        return result
    }
}

export { ControlPlaneClient }
export type { ControlPlaneOptions }
