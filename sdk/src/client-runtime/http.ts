import { z } from "zod"

import { ActorProtocolError } from "./errors.js"
import type { JsonValue } from "./json.js"
import { projectActorPath, validateOrigin } from "./settings.js"

export class HttpActorHostTransport implements ActorHostTransport {
    constructor(private readonly fetchRequest: typeof globalThis.fetch = globalThis.fetch) {}

    async invoke(target: ActorHostTarget, invocation: DirectActorInvocation): Promise<ActorHostReply> {
        const response = await this.post(target, invocation, "invoke", {
            requestId: invocation.requestId,
            ownerEpoch: target.ownerEpoch,
            method: invocation.method,
            args: invocation.args
        })
        // A 401 is issued before dispatch, so refreshing this ticket cannot repeat actor code.
        if (response.status === 401) return { type: "unauthenticated" }
        if (!response.ok) throw new Error(`actor host returned HTTP ${response.status}`)
        const reply = actorHostReplySchema.safeParse(await responseDocument(response))
        if (!reply.success) throw new ActorProtocolError("actor host response did not contain a valid outcome")
        return reply.data
    }

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

const actorHostReplySchema = z.discriminatedUnion("type", [
    z.object({ type: z.literal("completed"), result: z.unknown() }),
    z.object({ type: z.literal("failed"), code: z.string().min(1), message: z.string() }),
    z.object({ type: z.literal("reroute") })
])

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
export interface DirectActorInvocation extends ActorAddress {
    readonly requestId: string
    readonly method: string
    readonly args: readonly JsonValue[]
}
export type ActorHostReply = z.infer<typeof actorHostReplySchema> | { readonly type: "unauthenticated" }
export interface ActorHostTransport {
    invoke(target: ActorHostTarget, invocation: DirectActorInvocation): Promise<ActorHostReply>
    publish(target: ActorHostTarget, actor: ActorAddress, effects: readonly unknown[]): Promise<void>
}
