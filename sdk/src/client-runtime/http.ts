import { ActorProtocolError } from "./errors.js"
import type { JsonValue } from "./json.js"
import { projectActorPath, validateOrigin } from "./settings.js"

export class HttpActorHostTransport implements ActorHostTransport {
    constructor(private readonly fetchRequest: typeof globalThis.fetch = globalThis.fetch) {}

    async invoke(target: ActorHostTarget, invocation: DirectActorInvocation): Promise<ActorHostReply> {
        // TODO: Replace this extra round trip with durable idempotency keys for safe invocation retries.
        const readiness = await this.ping(target, invocation)
        if (readiness !== "ready") return { type: readiness }
        let response: Response
        try {
            response = await this.post(target, invocation, "invoke", {
                requestId: invocation.requestId,
                ownerEpoch: target.ownerEpoch,
                method: invocation.method,
                args: invocation.args
            })
        } catch (error) {
            if (connectionRefused(error)) return { type: "not_dispatched" }
            throw error
        }
        // A 401 is issued before dispatch, so refreshing this ticket cannot repeat actor code.
        if (response.status === 401) return { type: "unauthenticated" }
        if (!response.ok) throw new Error(`actor host returned HTTP ${response.status}`)
        const reply = await responseDocument(response)
        if (isRecord(reply)) {
            if (reply.type === "completed" && Object.hasOwn(reply, "result"))
                return { type: "completed", result: reply.result }
            if (
                reply.type === "failed" &&
                typeof reply.code === "string" &&
                reply.code.length > 0 &&
                typeof reply.message === "string"
            )
                return { type: "failed", code: reply.code, message: reply.message }
            if (reply.type === "reroute") return { type: "reroute" }
        }
        throw new ActorProtocolError("actor host response did not contain a valid outcome")
    }

    async publish(target: ActorHostTarget, actor: ActorAddress, effects: readonly unknown[]): Promise<void> {
        const response = await this.post(target, actor, "socket-effects", { ownerEpoch: target.ownerEpoch, effects })
        if (!response.ok) throw new Error(`socket effects returned HTTP ${response.status}`)
    }

    private async ping(
        target: ActorHostTarget,
        actor: ActorAddress
    ): Promise<"ready" | "unauthenticated" | "not_dispatched"> {
        try {
            const response = await this.fetchRequest(
                `${validateOrigin(target.route)}${projectActorPath(actor.projectId, actor.actorName, actor.actorId)}/invoke`,
                {
                    method: "HEAD",
                    redirect: "error",
                    headers: { authorization: `Bearer ${target.token}` },
                    signal: AbortSignal.timeout(5_000)
                }
            )
            if (response.status === 401) return "unauthenticated"
            return response.status === 204 ? "ready" : "not_dispatched"
        } catch {
            return "not_dispatched"
        }
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

function connectionRefused(error: unknown, ancestors = new Set<unknown>()): boolean {
    if (!isRecord(error) || ancestors.has(error)) return false
    const visited = new Set(ancestors).add(error)
    if (error.code !== undefined && error.code !== "ECONNREFUSED") return false
    if (error.errors !== undefined)
        return (
            Array.isArray(error.errors) &&
            error.errors.length > 0 &&
            error.errors.every(cause => connectionRefused(cause, visited))
        )
    if (error.code === "ECONNREFUSED") return error.syscall === "connect"
    return connectionRefused(error.cause, visited)
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
export interface DirectActorInvocation extends ActorAddress {
    readonly requestId: string
    readonly method: string
    readonly args: readonly JsonValue[]
}
export type ActorHostReply =
    | { readonly type: "completed"; readonly result: unknown }
    | { readonly type: "failed"; readonly code: string; readonly message: string }
    | { readonly type: "reroute" }
    | { readonly type: "unauthenticated" }
    | { readonly type: "not_dispatched" }
export interface ActorHostTransport {
    invoke(target: ActorHostTarget, invocation: DirectActorInvocation): Promise<ActorHostReply>
    publish(target: ActorHostTarget, actor: ActorAddress, effects: readonly unknown[]): Promise<void>
}
