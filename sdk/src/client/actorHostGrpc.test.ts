import { Server, ServerCredentials, loadPackageDefinition, status } from "@grpc/grpc-js"
import type { ServerUnaryCall, ServiceClientConstructor, sendUnaryData } from "@grpc/grpc-js"
import { loadSync } from "@grpc/proto-loader"
import assert from "node:assert/strict"
import { test } from "node:test"
import { fileURLToPath } from "node:url"

import { ActorProtocolError } from "../errors.js"

import { GrpcActorHostTransport } from "./actorHostGrpc.js"

test("direct transport speaks the actor host protobuf contract", async () => {
    const server = new Server()
    const definition = loadPackageDefinition(
        loadSync(fileURLToPath(new URL("../generated/durable_object.proto", import.meta.url)), {
            defaults: true,
            longs: Number,
            oneofs: true
        })
    ) as unknown as GrpcPackages
    server.addService(definition.durable_object.v1.ActorHostService.service, {
        invoke(call: ServerUnaryCall<HostRequest, HostReply>, callback: sendUnaryData<HostReply>) {
            assert.equal(call.metadata.get("authorization")[0], "Bearer direct-token")
            assert.deepEqual(call.request, {
                invocation: {
                    requestId: "request-1",
                    actor: { actorType: "Counter", actorId: "counter-1" },
                    method: "increment",
                    argsJson: Buffer.from("[2]")
                },
                ownerEpoch: 3
            })
            callback(null, {
                completed: { resultJson: Buffer.from("7"), socketEffectsJson: Buffer.from("[]") },
                result: "completed"
            })
        }
    })
    const port = await listen(server)
    const transport = new GrpcActorHostTransport()
    try {
        assert.deepEqual(
            await transport.invoke(
                {
                    route: `http://127.0.0.1:${port}`,
                    token: "direct-token",
                    ownerEpoch: 3,

                    expiresAtMs: 4_000_000_000_000
                },
                {
                    requestId: "request-1",
                    actorType: "Counter",
                    actorId: "counter-1",
                    method: "increment",
                    args: [2]
                }
            ),
            { type: "completed", result: 7, effects: [] }
        )
    } finally {
        server.forceShutdown()
    }
})

test("direct transport rejects structurally invalid socket effects", async () => {
    const server = actorHostServer({
        completed: {
            resultJson: Buffer.from("null"),
            socketEffectsJson: Buffer.from('{"type":"send"}')
        },
        result: "completed"
    })
    const port = await listen(server)
    const transport = new GrpcActorHostTransport()
    try {
        await assert.rejects(
            transport.invoke(
                {
                    route: `http://127.0.0.1:${port}`,
                    token: "direct-token",
                    ownerEpoch: 3,

                    expiresAtMs: 4_000_000_000_000
                },
                {
                    requestId: "request-1",
                    actorType: "Counter",
                    actorId: "counter-1",
                    method: "increment",
                    args: [2]
                }
            ),
            ActorProtocolError
        )
    } finally {
        server.forceShutdown()
    }
})

test("only transport authentication rejections are safe to retry", async () => {
    for (const code of [status.UNAUTHENTICATED, status.UNAVAILABLE, status.DEADLINE_EXCEEDED]) {
        const server = actorHostServer(
            {
                completed: { resultJson: Buffer.from("null"), socketEffectsJson: Buffer.from("[]") },
                result: "completed"
            },
            code
        )
        const port = await listen(server)
        const transport = new GrpcActorHostTransport()
        try {
            const request = transport.invoke(
                {
                    route: `http://127.0.0.1:${port}`,
                    token: "expired",
                    ownerEpoch: 1,

                    expiresAtMs: 1
                },
                {
                    requestId: "one",
                    actorType: "Counter",
                    actorId: "one",
                    method: "get",
                    args: []
                }
            )
            if (code === status.UNAUTHENTICATED) assert.deepEqual(await request, { type: "unauthenticated" })
            else await assert.rejects(request)
        } finally {
            server.forceShutdown()
        }
    }
})

function actorHostServer(reply: HostReply, errorCode?: number): Server {
    const server = new Server()
    const definition = loadPackageDefinition(
        loadSync(fileURLToPath(new URL("../generated/durable_object.proto", import.meta.url)), {
            defaults: true,
            longs: Number,
            oneofs: true
        })
    ) as unknown as GrpcPackages
    server.addService(definition.durable_object.v1.ActorHostService.service, {
        invoke(_call: ServerUnaryCall<HostRequest, HostReply>, callback: sendUnaryData<HostReply>) {
            if (errorCode !== undefined) return callback({ code: errorCode, message: "rejected" })
            callback(null, reply)
        }
    })
    return server
}

function listen(server: Server): Promise<number> {
    return new Promise((resolvePort, reject) => {
        server.bindAsync("127.0.0.1:0", ServerCredentials.createInsecure(), (error, port) => {
            if (error) reject(error)
            else resolvePort(port)
        })
    })
}

interface GrpcPackages {
    readonly durable_object: {
        readonly v1: {
            readonly ActorHostService: ServiceClientConstructor
        }
    }
}

interface HostRequest {
    readonly invocation: {
        readonly requestId: string
        readonly actor: { readonly actorType: string; readonly actorId: string }
        readonly method: string
        readonly argsJson: Buffer
    }
    readonly ownerEpoch: number
}

type HostReply = {
    readonly completed: { readonly resultJson: Buffer; readonly socketEffectsJson: Buffer }
    readonly result: "completed"
}

test("socket effects use authenticated host gRPC with actor ownership binding", async () => {
    const server = new Server()
    const definition = loadPackageDefinition(
        loadSync(fileURLToPath(new URL("../generated/durable_object.proto", import.meta.url)), {
            defaults: true,
            longs: Number,
            oneofs: true
        })
    ) as unknown as GrpcPackages
    let delivered = false
    server.addService(definition.durable_object.v1.ActorHostService.service, {
        publishSocketEffects(
            call: ServerUnaryCall<
                { actor: { actorType: string; actorId: string }; ownerEpoch: number; effectsJson: Buffer },
                object
            >,
            callback: sendUnaryData<object>
        ) {
            assert.equal(call.metadata.get("authorization")[0], "Bearer actor-token")
            assert.deepEqual(call.request.actor, { actorType: "Room", actorId: "lobby" })
            assert.equal(call.request.ownerEpoch, 7)
            assert.deepEqual(JSON.parse(call.request.effectsJson.toString()), [
                { type: "broadcast", message: { type: "text", data: '"hello"' }, except_connection_ids: [], tags: [] }
            ])
            delivered = true
            callback(null, {})
        }
    })
    const port = await listen(server)
    try {
        await new GrpcActorHostTransport().publish(
            { route: `http://127.0.0.1:${port}`, token: "actor-token", ownerEpoch: 7, expiresAtMs: 4_000_000_000_000 },
            { actorType: "Room", actorId: "lobby" },
            [{ type: "broadcast", message: { type: "text", data: '"hello"' }, except_connection_ids: [], tags: [] }]
        )
        assert.equal(delivered, true)
    } finally {
        server.forceShutdown()
    }
})
