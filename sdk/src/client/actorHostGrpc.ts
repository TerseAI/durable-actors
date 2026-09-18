import { Metadata, credentials, loadPackageDefinition, status } from "@grpc/grpc-js"
import { loadSync } from "@grpc/proto-loader"
import { fileURLToPath } from "node:url"

import { parseSocketEffects } from "../actor/socketProtocol.js"
import type { SocketEffect } from "../actor/socketProtocol.js"
import { ActorProtocolError } from "../errors.js"
import type { ProtoGrpcType } from "../generated/durable_object.js"
import type { ActorHostServiceClient } from "../generated/durable_object/v1/ActorHostService.js"
import type { HostInvokeActorRequest } from "../generated/durable_object/v1/HostInvokeActorRequest.js"
import type { InvokeActorReply__Output } from "../generated/durable_object/v1/InvokeActorReply.js"
import type { JsonValue } from "../json.js"

const MAX_MESSAGE_BYTES = 32 * 1024 * 1024

interface ActorHostTarget {
    readonly route: string
    readonly token: string
    readonly ownerEpoch: number

    readonly expiresAtMs: number
}

interface DirectActorInvocation {
    readonly requestId: string
    readonly actorType: string
    readonly actorId: string
    readonly method: string
    readonly args: readonly JsonValue[]
}

type ActorHostReply =
    | { readonly type: "completed"; readonly result: unknown; readonly effects: readonly SocketEffect[] }
    | { readonly type: "failed"; readonly code: string; readonly message: string }
    | { readonly type: "reroute" }
    | { readonly type: "unauthenticated" }

interface ActorHostTransport {
    publish(
        target: ActorHostTarget,
        actor: { actorType: string; actorId: string },
        effects: readonly SocketEffect[]
    ): Promise<void>
    invoke(target: ActorHostTarget, invocation: DirectActorInvocation): Promise<ActorHostReply>
}

class GrpcActorHostTransport implements ActorHostTransport {
    private readonly clients = new Map<string, ActorHostServiceClient>()

    async invoke(target: ActorHostTarget, invocation: DirectActorInvocation): Promise<ActorHostReply> {
        const metadata = new Metadata()
        metadata.set("authorization", `Bearer ${target.token}`)
        const request: HostInvokeActorRequest = {
            invocation: {
                requestId: invocation.requestId,
                actor: {
                    actorType: invocation.actorType,
                    actorId: invocation.actorId
                },
                method: invocation.method,
                argsJson: Buffer.from(JSON.stringify(invocation.args))
            },
            ownerEpoch: target.ownerEpoch
        }
        try {
            const reply = await unaryRequest(this.client(target.route), request, metadata)
            return decodeReply(reply)
        } catch (error) {
            // The host authenticates before dispatching any actor code.
            if (error instanceof Error && "code" in error && error.code === status.UNAUTHENTICATED)
                return { type: "unauthenticated" }
            throw error
        }
    }

    async publish(
        target: ActorHostTarget,
        actor: { actorType: string; actorId: string },
        effects: readonly SocketEffect[]
    ): Promise<void> {
        const metadata = new Metadata()
        metadata.set("authorization", `Bearer ${target.token}`)
        await new Promise<void>((resolve, reject) => {
            this.client(target.route).publishSocketEffects(
                { actor, ownerEpoch: target.ownerEpoch, effectsJson: Buffer.from(JSON.stringify(effects)) },
                metadata,
                error => (error ? reject(error) : resolve())
            )
        })
    }

    private client(route: string): ActorHostServiceClient {
        const existing = this.clients.get(route)
        if (existing) return existing
        const url = actorHostUrl(route)
        const address = url.port ? url.host : `${url.hostname}:${url.protocol === "https:" ? 443 : 80}`
        const client = new ActorHostClient(
            address,
            url.protocol === "https:" ? credentials.createSsl() : credentials.createInsecure(),
            {
                "grpc.max_receive_message_length": MAX_MESSAGE_BYTES,
                "grpc.max_send_message_length": MAX_MESSAGE_BYTES
            }
        )
        this.clients.set(route, client)
        return client
    }
}

function unaryRequest(
    client: ActorHostServiceClient,
    request: HostInvokeActorRequest,
    metadata: Metadata
): Promise<InvokeActorReply__Output> {
    return new Promise((resolve, reject) => {
        client.invoke(request, metadata, (error, reply) => {
            if (error) reject(error)
            else if (reply) resolve(reply)
            else reject(new ActorProtocolError("actor host gRPC response was empty"))
        })
    })
}

function decodeReply(reply: InvokeActorReply__Output): ActorHostReply {
    if (reply.completed) {
        return {
            type: "completed",
            result: parseJson(reply.completed.resultJson, "result"),
            effects: parseSocketEffects(parseJson(reply.completed.socketEffectsJson, "socket effects"))
        }
    }
    if (reply.failed) return { type: "failed", code: reply.failed.code, message: reply.failed.message }
    if (reply.reroute) return { type: "reroute" }
    throw new ActorProtocolError("actor host response did not contain a result")
}

function parseJson(bytes: Uint8Array, label: string): unknown {
    try {
        return JSON.parse(Buffer.from(bytes).toString("utf8")) as unknown
    } catch (error) {
        throw new ActorProtocolError(`actor host ${label} was not valid JSON`, { cause: error })
    }
}

function actorHostUrl(route: string): URL {
    const url = new URL(route)
    if (
        !/^https?:$/u.test(url.protocol) ||
        !url.hostname ||
        url.username ||
        url.password ||
        url.pathname !== "/" ||
        url.search ||
        url.hash
    ) {
        throw new ActorProtocolError("actor host route must be an HTTP or HTTPS origin")
    }
    return url
}

const definition = loadPackageDefinition(
    loadSync(fileURLToPath(new URL("../generated/durable_object.proto", import.meta.url)), {
        defaults: true,
        longs: Number,
        oneofs: true
    })
) as unknown as ProtoGrpcType
const ActorHostClient = definition.durable_object.v1.ActorHostService

export { GrpcActorHostTransport }
export type { ActorHostReply, ActorHostTarget, ActorHostTransport, DirectActorInvocation }
