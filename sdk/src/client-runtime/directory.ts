import { ActorProtocolError } from "./errors.js"
import { isRecord, responseDocument } from "./http.js"
import { authorizationHeaders, configuredSettings, projectActorPath, validateActorComponent } from "./settings.js"
import type { DurableActorsClientOptions } from "./settings.js"

export type ActorHomeRegion = "north-america-west" | "north-america-central" | "north-america-east"

export interface ActorObject {
    readonly objectId: string
    readonly projectId: string
    readonly actorName: string
    readonly actorId: string
    readonly homeRegion: ActorHomeRegion
    readonly firstIngressRegion: ActorHomeRegion
}

/** Resolves permanent object identities through the regional gateway. Keep this client on the server. */
export class ActorDirectoryClient {
    private readonly settings: ReturnType<typeof configuredSettings>

    constructor(
        options: DurableActorsClientOptions,
        private readonly fetchRequest: typeof globalThis.fetch = globalThis.fetch
    ) {
        this.settings = configuredSettings(options)
    }

    async resolve(actorName: string, actorId: string): Promise<ActorObject> {
        validateActorComponent("actor name", actorName)
        validateActorComponent("actor ID", actorId)
        const path = `${projectActorPath(this.settings.projectId, actorName, actorId)}/resolve`
        const response = await this.request(path, "POST")
        const object = this.parse(await responseDocument(response))
        if (object.actorName !== actorName || object.actorId !== actorId)
            throw new ActorProtocolError("directory response has a different actor scope")
        return object
    }

    async get(objectId: string): Promise<ActorObject | null> {
        validateObjectId(objectId)
        const response = await this.request(`/v1/projects/${this.settings.projectId}/objects/${objectId}`, "GET")
        if (response.status === 404) return null
        const object = this.parse(await responseDocument(response))
        if (object.objectId !== objectId)
            throw new ActorProtocolError("directory response has a different object scope")
        return object
    }

    private async request(path: string, method: "GET" | "POST"): Promise<Response> {
        const response = await this.fetchRequest(`${this.settings.controlPlaneUrl}${path}`, {
            method,
            redirect: "error",
            headers: { ...authorizationHeaders(this.settings.credential), accept: "application/json" }
        })
        if (!response.ok && !(method === "GET" && response.status === 404))
            throw new ActorProtocolError(`directory request returned HTTP ${response.status}`)
        return response
    }

    private parse(value: unknown): ActorObject {
        if (
            !isRecord(value) ||
            typeof value.objectId !== "string" ||
            !isRecord(value.actor) ||
            typeof value.actor.actor_name !== "string" ||
            typeof value.actor.actor_id !== "string" ||
            !isRegion(value.homeRegion) ||
            value.firstIngressRegion !== value.homeRegion
        )
            throw new ActorProtocolError("directory response is invalid")
        if (value.actor.project_id !== this.settings.projectId)
            throw new ActorProtocolError("directory response has a different project scope")
        validateObjectId(value.objectId)
        return {
            objectId: value.objectId,
            projectId: this.settings.projectId,
            actorName: validateActorComponent("actor name", value.actor.actor_name),
            actorId: validateActorComponent("actor ID", value.actor.actor_id),
            homeRegion: value.homeRegion,
            firstIngressRegion: value.homeRegion
        }
    }
}

function validateObjectId(value: string): void {
    if (!/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u.test(value))
        throw new ActorProtocolError("invalid global object ID")
}

function isRegion(value: unknown): value is ActorHomeRegion {
    return value === "north-america-west" || value === "north-america-central" || value === "north-america-east"
}
