import assert from "node:assert/strict"
import { createServer } from "node:http"
import type { Server, ServerResponse } from "node:http"
import { performance } from "node:perf_hooks"
import { test } from "node:test"

import type { ActorConnection } from "../actor/socket.js"
import { ActorInvocationError } from "../errors.js"

import { RemoteActorClient } from "./remoteClient.js"

test("API-key clients invoke, connect, and broadcast", async () => {
    const requests: string[] = []
    const client = new RemoteActorClient(undefined, {
        environment: {
            DURABLE_OBJECT_API_KEY: "backend-key",
            DURABLE_OBJECT_CONTROL_PLANE_URL: "https://control.example.com"
        },
        telemetry: () => {},
        fetch: async (url, options) => {
            requests.push(String(url))
            assert.equal(new Headers(options?.headers).get("authorization"), "Bearer backend-key")
            if (JSON.parse(String(options?.body)).transport === "websocket")
                return Response.json({
                    websocketUrl: "wss://host.example.com/v1/socket?key=socket-ticket",
                    key: "socket-ticket"
                })
            return Response.json({
                route: "https://host.example.com",
                token: "invocation-ticket",
                ownerEpoch: 1,

                expiresAtMs: 4_000_000_000_000
            })
        },
        actorHost: {
            async publish() {},
            async invoke(target, invocation) {
                assert.equal(target.token, "invocation-ticket")
                assert.equal(invocation.actorId, "one")
                return { type: "completed", result: 7, effects: [] }
            }
        },
        async connectWebSocket(url) {
            assert.equal(url, "wss://host.example.com/v1/socket?key=socket-ticket")
            return {} as ActorConnection
        }
    })
    assert.equal(await client.invoke("Counter", "one", "increment", []), 7)
    await client.connect("Counter", "one", {})
    await client.broadcast("Counter", "one", "updated")
    assert.deepEqual(requests, [
        "https://control.example.com/v1/actors/Counter/one/connect",
        "https://control.example.com/v1/actors/Counter/one/connect"
    ])
})

test("target expiry uses real time even when workflow Date.now is frozen", async () => {
    const originalNow = Date.now
    let resolutions = 0
    const usedTokens: string[] = []
    Date.now = () => 1
    try {
        const client = new RemoteActorClient(
            { apiKey: "backend-key", controlPlaneUrl: "https://control.example.com" },
            {
                telemetry: () => {},
                fetch: async () =>
                    Response.json({
                        route: "https://host.example.com",
                        token: `target-${++resolutions}`,
                        ownerEpoch: 1,

                        expiresAtMs: Math.floor(performance.timeOrigin + performance.now()) + 1_000
                    }),
                actorHost: {
                    async publish() {},
                    async invoke(target) {
                        usedTokens.push(target.token)
                        return { type: "completed", result: null, effects: [] }
                    }
                }
            }
        )
        await client.invoke("Counter", "one", "get", [])
        await client.invoke("Counter", "one", "get", [])
        assert.deepEqual(usedTokens, ["target-1", "target-2"])
    } finally {
        Date.now = originalNow
    }
})

test("refreshes a rejected actor ticket once using the same invocation ID", async () => {
    let resolutions = 0
    let calls = 0
    let rejectAll = false
    const client = new RemoteActorClient(
        { apiKey: "backend-key", controlPlaneUrl: "https://control.example.com" },
        {
            telemetry: () => {},
            requestId: () => "same-request",
            fetch: async () =>
                Response.json({
                    route: "https://host.example.com",
                    token: `target-${++resolutions}`,
                    ownerEpoch: 1,

                    expiresAtMs: 4_000_000_000_000
                }),
            actorHost: {
                async publish() {},
                async invoke(_target, invocation) {
                    calls++
                    assert.equal(invocation.requestId, "same-request")
                    if (calls === 1 || rejectAll) return { type: "unauthenticated" }
                    return { type: "completed", result: 7, effects: [] }
                }
            }
        }
    )
    assert.equal(await client.invoke("Counter", "one", "increment", [1]), 7)
    assert.equal(resolutions, 2)
    assert.equal(calls, 2)
    rejectAll = true
    await assert.rejects(
        client.invoke("Counter", "one", "increment", [1]),
        error => error instanceof ActorInvocationError && error.code === "unauthenticated"
    )
    assert.equal(calls, 4)
    assert.equal(resolutions, 3)
})

test("does not retry ambiguous host failures or actor-method authentication errors", async () => {
    for (const ambiguous of [true, false]) {
        let calls = 0
        const client = new RemoteActorClient(
            { apiKey: "backend-key", controlPlaneUrl: "https://control.example.com" },
            {
                telemetry: () => {},
                fetch: async () =>
                    Response.json({
                        route: "https://host.example.com",
                        token: "target",
                        ownerEpoch: 1,

                        expiresAtMs: 4_000_000_000_000
                    }),
                actorHost: {
                    async publish() {},
                    async invoke() {
                        calls++
                        if (ambiguous) throw new Error("connection lost")
                        return { type: "failed", code: "unauthenticated", message: "actor method failed" }
                    }
                }
            }
        )
        await assert.rejects(
            client.invoke("Counter", "one", "increment", [1]),
            error =>
                error instanceof ActorInvocationError &&
                error.code === (ambiguous ? "outcome_unknown" : "unauthenticated")
        )
        assert.equal(calls, 1)
    }
})

test("remote actor client resolves once and invokes the actor host directly", async () => {
    let resolutions = 0
    const hostInvocations: unknown[] = []
    const telemetry: unknown[] = []
    const server = createServer(async (request, response) => {
        resolutions += 1
        assert.equal(request.method, "POST")
        assert.equal(request.url, "/v1/actors/Counter/counter-1/connect")
        assert.equal(request.headers.authorization, "Bearer backend-key")
        assert.equal(request.headers["x-request-id"], "00000000-0000-4000-8000-000000000000")
        json(response, 200, {
            route: "https://actor.example.com",
            token: "direct-token",
            ownerEpoch: 3,

            expiresAtMs: 4_000_000_000_000
        })
    })
    const port = await listen(server)
    const client = new RemoteActorClient(
        {
            apiKey: "backend-key",
            controlPlaneUrl: `http://127.0.0.1:${port}`
        },
        {
            requestId: () => "00000000-0000-4000-8000-000000000000",
            actorHost: {
                async publish() {},
                async invoke(target, invocation) {
                    hostInvocations.push({ target, invocation })
                    return { type: "completed", result: 7, effects: [] }
                }
            },
            monotonicNow: tickingClock(),
            telemetry: event => telemetry.push(event)
        }
    )
    try {
        assert.equal(await client.invoke("Counter", "counter-1", "increment", [2]), 7)
        assert.equal(await client.invoke("Counter", "counter-1", "increment", [3]), 7)
        assert.equal(resolutions, 1)
        assert.equal(hostInvocations.length, 2)
        assert.deepEqual(telemetry, [
            {
                event: "actor_client_invocation",
                request_id: "00000000-0000-4000-8000-000000000000",
                actor_type: "Counter",
                actor_id: "counter-1",
                method: "increment",
                started_at_ms: 0,
                invocation_built_at_ms: 1,
                target_cache_checked_at_ms: 2,
                target_resolved_at_ms: 3,
                host_rpc_completed_at_ms: 4,
                socket_effects_completed_at_ms: 5,
                completed_at_ms: 6,
                outcome: "completed"
            },
            {
                event: "actor_client_invocation",
                request_id: "00000000-0000-4000-8000-000000000000",
                actor_type: "Counter",
                actor_id: "counter-1",
                method: "increment",
                started_at_ms: 0,
                invocation_built_at_ms: 1,
                target_cache_checked_at_ms: 2,
                target_resolved_at_ms: 3,
                host_rpc_completed_at_ms: 4,
                socket_effects_completed_at_ms: 5,
                completed_at_ms: 6,
                outcome: "completed"
            }
        ])
        assert.deepEqual(hostInvocations[0], {
            target: {
                route: "https://actor.example.com",
                token: "direct-token",
                ownerEpoch: 3,

                expiresAtMs: 4_000_000_000_000
            },
            invocation: {
                requestId: "00000000-0000-4000-8000-000000000000",
                actorType: "Counter",
                actorId: "counter-1",
                method: "increment",
                args: [2]
            }
        })
    } finally {
        await close(server)
    }
})

test("resolves a fresh host socket grant before connecting", async () => {
    const requests: unknown[] = []
    const connection = fakeConnection()
    const client = new RemoteActorClient(
        {
            apiKey: "backend-key",
            controlPlaneUrl: "https://control.example.com"
        },
        {
            requestId: () => "connection-request",
            fetch: async (url, init) => {
                assert.equal(String(url), "https://control.example.com/v1/actors/ChatRoom/room-1/connect")
                assert.equal(JSON.parse(init!.body as string).backend, true)
                return Response.json({
                    websocketUrl: "wss://host.modal.test/v1/socket?key=host-ticket",
                    key: "host-ticket"
                })
            },
            connectWebSocket: async (url, metadata) => {
                requests.push({ url, metadata })
                return connection
            }
        }
    )
    assert.equal(await client.connect("ChatRoom", "room-1", { userId: "user-1" }), connection)
    assert.deepEqual(requests, [
        {
            url: "wss://host.modal.test/v1/socket?key=host-ticket",
            metadata: { userId: "user-1" }
        }
    ])
})

test("delivers returned effects to the same host and does not repeat a committed method on delivery failure", async () => {
    let invocations = 0
    let deliveries = 0
    let reject = false
    const client = new RemoteActorClient(
        { apiKey: "key", controlPlaneUrl: "https://control.example" },
        {
            telemetry: () => {},
            fetch: async url => {
                assert.equal(String(url), "https://control.example/v1/actors/Room/one/connect")
                return Response.json({
                    route: "https://host.example",
                    token: "ticket",
                    ownerEpoch: 3,
                    expiresAtMs: 4_000_000_000_000
                })
            },
            actorHost: {
                async invoke() {
                    invocations++
                    return {
                        type: "completed",
                        result: 7,
                        effects: [
                            {
                                type: "broadcast",
                                message: { type: "text", data: "7" },
                                except_connection_ids: [],
                                tags: []
                            }
                        ]
                    }
                },
                async publish(target, actor, effects) {
                    deliveries++
                    assert.equal(target.route, "https://host.example")
                    assert.equal(target.ownerEpoch, 3)
                    assert.equal(actor.actorId, "one")
                    assert.equal(effects[0]?.type, "broadcast")
                    if (reject) throw new Error("lost delivery acknowledgement")
                }
            }
        }
    )
    assert.equal(await client.invoke("Room", "one", "announce", []), 7)
    reject = true
    await assert.rejects(
        client.invoke("Room", "one", "announce", []),
        error => error instanceof ActorInvocationError && error.code === "outcome_unknown"
    )
    assert.equal(invocations, 2)
    assert.equal(deliveries, 2)
})

test("does not retry a control-plane transport failure", async () => {
    let calls = 0
    const server = createServer(request => {
        calls += 1
        request.socket.destroy()
    })
    const port = await listen(server)
    const client = new RemoteActorClient({
        apiKey: "backend-key",
        controlPlaneUrl: `http://127.0.0.1:${port}`
    })
    try {
        await assert.rejects(
            client.invoke("Counter", "counter-1", "increment", [2]),
            error => error instanceof ActorInvocationError && error.code === "outcome_unknown"
        )
        assert.equal(calls, 1)
    } finally {
        await close(server)
    }
})

test("requires the direct actor target endpoint", async () => {
    const calls: string[] = []
    const server = createServer((request, response) => {
        calls.push(request.url ?? "")
        json(response, 404, { error: { code: "not_found", message: "not found" } })
    })
    const port = await listen(server)
    const client = new RemoteActorClient({
        apiKey: "backend-key",
        controlPlaneUrl: `http://127.0.0.1:${port}`
    })
    try {
        await assert.rejects(
            client.invoke("Counter", "counter-1", "increment", [2]),
            error => error instanceof ActorInvocationError && error.code === "not_found"
        )
        assert.deepEqual(calls, ["/v1/actors/Counter/counter-1/connect"])
    } finally {
        await close(server)
    }
})

test("preserves a structured actor failure from HTTP", async () => {
    const server = createServer((_request, response) => {
        json(response, 422, {
            error: {
                code: "actor_error",
                message: "actor method failed",
                requestId: "server-request"
            }
        })
    })
    const port = await listen(server)
    const client = new RemoteActorClient({
        apiKey: "backend-key",
        controlPlaneUrl: `http://127.0.0.1:${port}`
    })
    try {
        await assert.rejects(
            client.invoke("Counter", "counter-1", "increment", [2]),
            error =>
                error instanceof ActorInvocationError &&
                error.code === "actor_error" &&
                error.requestId === "server-request"
        )
    } finally {
        await close(server)
    }
})

function json(response: ServerResponse, status: number, body: unknown): void {
    response.writeHead(status, { "content-type": "application/json" })
    response.end(JSON.stringify(body))
}

function listen(server: Server): Promise<number> {
    return new Promise((resolve, reject) => {
        server.once("error", reject)
        server.listen(0, "127.0.0.1", () => {
            server.off("error", reject)
            const address = server.address()
            if (address === null || typeof address === "string")
                reject(new Error("test HTTP server has no TCP address"))
            else resolve(address.port)
        })
    })
}

function close(server: Server): Promise<void> {
    return new Promise((resolve, reject) => server.close(error => (error ? reject(error) : resolve())))
}

async function requestBody(request: import("node:http").IncomingMessage): Promise<unknown> {
    const chunks: Buffer[] = []
    for await (const chunk of request) chunks.push(Buffer.from(chunk))
    return chunks.length === 0 ? undefined : (JSON.parse(Buffer.concat(chunks).toString("utf8")) as unknown)
}

function fakeConnection(): ActorConnection {
    return {
        readyState: 1,
        send: () => undefined,
        close: () => undefined,
        addEventListener: () => undefined,
        removeEventListener: () => undefined
    }
}

function tickingClock(): () => number {
    let current = 0
    return () => current++
}

test("broadcasts use the owning host gRPC connection without HTTP delivery", async () => {
    let published = false
    const client = new RemoteActorClient(
        { apiKey: "backend-key", controlPlaneUrl: "https://control.example" },
        {
            telemetry: () => {},
            fetch: async (url, options) => {
                assert.equal(String(url), "https://control.example/v1/actors/Room/lobby/connect")
                assert.equal(JSON.parse(String(options?.body)).transport, "grpc")
                return Response.json({
                    transport: "grpc",
                    homeRegion: "north-america-west",
                    route: "https://host.example",
                    token: "actor",
                    ownerEpoch: 7,
                    expiresAtMs: 4_000_000_000_000
                })
            },
            actorHost: {
                ...{
                    async publish(
                        target: { ownerEpoch: number },
                        actor: { actorId: string },
                        effects: readonly unknown[]
                    ) {
                        assert.equal(target.ownerEpoch, 7)
                        assert.equal(actor.actorId, "lobby")
                        assert.deepEqual(effects, [
                            {
                                type: "broadcast",
                                message: { type: "text", data: '"hello"' },
                                except_connection_ids: [],
                                tags: []
                            }
                        ])
                        published = true
                    }
                },
                async invoke() {
                    throw new Error("broadcast must not invoke an actor method")
                }
            }
        }
    )
    await client.broadcast("Room", "lobby", "hello")
    assert.equal(published, true)
})
