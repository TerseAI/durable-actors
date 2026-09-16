import { configuredSettings } from "../client/clientSettings.js"
import { readLocalSettings } from "../client/localSettings.js"

interface ControlPlaneOptions {
    url?: string | true
    apiKey?: string
    namespace?: string
}

class ControlPlaneClient {
    readonly connection: ReturnType<typeof configuredSettings>

    constructor(
        options: ControlPlaneOptions,
        private readonly request: typeof fetch,
        readLocal: typeof readLocalSettings = readLocalSettings
    ) {
        const url = typeof options.url === "string" ? options.url : process.env.DURABLE_OBJECT_CONTROL_PLANE_URL
        const key = options.apiKey || process.env.DURABLE_OBJECT_API_KEY
        const local = !url && !key ? readLocal() : {}
        const controlPlaneUrl = url || local.controlPlaneUrl
        const apiKey = key || local.apiKey
        if (!controlPlaneUrl)
            throw new Error("Start `npx little-actors dev` first, or set --url or DURABLE_OBJECT_CONTROL_PLANE_URL.")
        if (!apiKey) throw new Error("An admin API key is required. Set DURABLE_OBJECT_API_KEY or --api-key.")
        this.connection = configuredSettings({
            controlPlaneUrl,
            apiKey,
            namespaceId: options.namespace || process.env.DURABLE_OBJECT_NAMESPACE_ID || local.namespaceId
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
