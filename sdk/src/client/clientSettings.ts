import { z } from "zod"

import { projectIdSchema } from "../actor/identity.js"
import { ActorConfigurationError } from "../errors.js"

function configuredSettings(options: unknown) {
    const result = clientOptionsSchema.safeParse(options)
    if (!result.success)
        throw new ActorConfigurationError(`durable-actors client settings are invalid: ${result.error.message}`)
    const controlPlaneUrl = validateOrigin(result.data.controlPlaneUrl)
    const local = ["localhost", "127.0.0.1", "[::1]"].includes(new URL(controlPlaneUrl).hostname)
    const projectId = result.data.projectId ?? (local ? "local" : undefined)
    if (projectId === undefined)
        throw new ActorConfigurationError(
            "durable-actors client settings are invalid: projectId is required for remote connections"
        )
    if (!local && result.data.apiKey === undefined)
        throw new ActorConfigurationError(
            "durable-actors client settings are invalid: apiKey (shared secret) is required for remote connections"
        )
    return {
        credential: result.data.apiKey,
        projectId,
        homeRegion: result.data.homeRegion,
        controlPlaneUrl
    }
}

function validateOrigin(origin: string): string {
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
    ) {
        throw new ActorConfigurationError(`actor control-plane URL must be an HTTP or HTTPS origin: ${origin}`)
    }
    return url.origin
}

const clientOptionsSchema = z.strictObject({
    projectId: projectIdSchema.optional(),
    apiKey: z.string().trim().min(1).optional(),
    homeRegion: z
        .string()
        .regex(/^[A-Za-z0-9._-]+$/u)
        .optional(),
    controlPlaneUrl: z.string().url()
})

function authorizationHeaders(credential: string | undefined): Record<string, string> {
    return credential === undefined ? {} : { authorization: `Bearer ${credential}` }
}

export { authorizationHeaders, configuredSettings }
