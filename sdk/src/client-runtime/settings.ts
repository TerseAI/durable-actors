import { ActorConfigurationError, ActorValidationError } from "./errors.js"

export interface DurableActorsClientOptions {
    readonly projectId?: string
    readonly apiKey?: string
    readonly homeRegion?: string
    readonly controlPlaneUrl: string
}

export type Environment = Record<string, string | undefined>

export function configuredSettings(value: unknown) {
    if (
        typeof value !== "object" ||
        value === null ||
        Array.isArray(value) ||
        Object.keys(value).some(key => !["projectId", "apiKey", "homeRegion", "controlPlaneUrl"].includes(key))
    )
        throw new ActorConfigurationError("durable-actors client settings are invalid")
    const options = value as DurableActorsClientOptions
    const controlPlaneUrl = validateOrigin(options.controlPlaneUrl)
    const local = ["localhost", "127.0.0.1", "[::1]"].includes(new URL(controlPlaneUrl).hostname)
    const projectId = options.projectId ?? (local ? "local" : undefined)
    if (typeof projectId !== "string" || !/^[A-Za-z0-9._-]{1,64}$/u.test(projectId) || [".", ".."].includes(projectId))
        throw new ActorConfigurationError(
            "durable-actors client settings are invalid: a valid projectId is required for remote connections"
        )
    if (options.apiKey !== undefined && (typeof options.apiKey !== "string" || !options.apiKey.trim()))
        throw new ActorConfigurationError("durable-actors client settings are invalid: apiKey must not be empty")
    if (options.homeRegion !== undefined) validateActorComponent("home region", options.homeRegion)
    return { projectId, credential: options.apiKey?.trim(), homeRegion: options.homeRegion, controlPlaneUrl }
}

export function environmentSettings(environment: Environment = runtimeEnvironment()): DurableActorsClientOptions {
    return {
        projectId: environment.DURABLE_ACTORS_PROJECT_ID,
        apiKey: environment.DURABLE_ACTORS_SECRET ?? environment.DURABLE_ACTORS_API_KEY,
        homeRegion: environment.DURABLE_ACTORS_HOME_REGION,
        controlPlaneUrl: environment.DURABLE_ACTORS_CONTROL_PLANE_URL ?? "http://127.0.0.1:7100"
    }
}

export function runtimeEnvironment(): Environment {
    return (globalThis as typeof globalThis & { process?: { env?: Environment } }).process?.env ?? {}
}

export function validateOrigin(origin: string): string {
    let url: URL
    try {
        url = new URL(origin)
    } catch (error) {
        throw new ActorConfigurationError(`actor HTTP origin is invalid: ${origin}`, { cause: error })
    }
    if (
        !/^https?:$/u.test(url.protocol) ||
        !url.hostname ||
        url.username ||
        url.password ||
        url.pathname !== "/" ||
        url.search ||
        url.hash
    )
        throw new ActorConfigurationError(`actor URL must be an HTTP or HTTPS origin: ${origin}`)
    return url.origin
}

export function validateActorComponent(name: string, value: string): string {
    if (typeof value !== "string" || !/^[A-Za-z0-9._-]+$/u.test(value) || value === "." || value === "..")
        throw new ActorValidationError(
            `${name} may contain only ASCII letters, digits, '.', '-', and '_' and must not be a path segment`
        )
    return value
}

export function projectActorPath(projectId: string, actorName: string, actorId: string): string {
    return `/v1/projects/${encodeURIComponent(validateActorComponent("project ID", projectId))}/actors/${encodeURIComponent(validateActorComponent("actor name", actorName))}/${encodeURIComponent(validateActorComponent("actor ID", actorId))}`
}

export function authorizationHeaders(credential: string | undefined): Record<string, string> {
    return credential === undefined ? {} : { authorization: `Bearer ${credential}` }
}
