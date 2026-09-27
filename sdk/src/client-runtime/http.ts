import { ActorProtocolError } from "./errors.js"
import type { JsonValue } from "./json.js"
import { projectActorPath, validateOrigin } from "./settings.js"

export class HttpActorHostTransport implements ActorHostTransport {
    constructor(private readonly fetchRequest: typeof globalThis.fetch = globalThis.fetch) {}

    async publish(target: ActorHostTarget, actor: ActorAddress, effects: readonly unknown[]): Promise<void> {
        const response = await this.post(target, actor, "socket-effects", { ownerEpoch: target.ownerEpoch, effects })
        if (!response.ok) throw new Error(`socket effects returned HTTP ${response.status}`)
    }

    private post(target: ActorHostTarget, actor: ActorAddress, endpoint: string, body: unknown): Promise<Response> {
        return this.fetchRequest(
            `${validateOrigin(target.route)}${projectActorPath(actor.projectId, actor.actorName, actor.actorId)}/${endpoint}`,
            {
                method: "POST",
                redirect: "error",
                headers: {
                    authorization: `Bearer ${target.token}`,
                    "content-type": "application/json",
                    accept: "application/json"
                },
                body: JSON.stringify(body)
            }
        )
    }
}

export async function responseDocument(response: Response): Promise<unknown> {
    try {
        return await response.json()
    } catch (error) {
        throw new ActorProtocolError(`HTTP ${response.status} response was not valid JSON`, { cause: error })
    }
}

export function isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === "object" && value !== null && !Array.isArray(value)
}

export interface ActorAddress {
    readonly projectId: string
    readonly actorName: string
    readonly actorId: string
}
export interface ActorHostTarget {
    readonly route: string
    readonly token: string
    readonly ownerEpoch: number
    readonly expiresAtMs: number
}
export interface ActorInvocation extends ActorAddress {
    readonly requestId: string
    readonly method: string
    readonly args: readonly JsonValue[]
}
export interface ActorHostTransport {
    publish(target: ActorHostTarget, actor: ActorAddress, effects: readonly unknown[]): Promise<void>
}
