import assert from "node:assert/strict"
import { createServer } from "node:http"
import type { Server, ServerResponse } from "node:http"
import { performance } from "node:perf_hooks"
import { test } from "node:test"

import type { ActorConnection } from "../actor/socket.js"
import { ActorInvocationError } from "../errors.js"

import { RemoteActorClient } from "./remoteClient.js"

test("API-key clients invoke, connect, and broadcast without a namespace or session token", async () => {
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
            if (String(url).endsWith("socket-effects")) return new Response(null, { status: 204 })
            return Response.json({
                namespaceId: "local",
                route: "https://host.example.com",
                token: "invocation-ticket",
                ownerEpoch: 1,

                expiresAtMs: 4_000_000_000_000
            })
        },
        actorHost: {
            async invoke(target, invocation) {
                assert.equal(target.token, "invocation-ticket")
                assert.equal(invocation.namespaceId, "local")
                return { type: "completed", result: 7, effects: [] }
            }
        },
        async connectWebSocket(url, credential) {
            assert.equal(url, "wss://control.example.com/v1/actors/Counter/one/websocket")
            assert.equal(credential, "backend-key")
            return {} as ActorConnection
        }
    })
    assert.equal(await client.invoke("Counter", "one", "increment", []), 7)
    await client.connect("Counter", "one", {})
    await client.broadcast("Counter", "one", "updated")
    assert.deepEqual(requests, [
        "https://control.example.com/v1/actors/Counter/one/target",
        "https://control.example.com/v1/actors/Counter/one/socket-effects"
    ])
})

test("delegated clients can let the server resolve the namespace from their session token", async () => {
    const client = new RemoteActorClient(undefined, {
        environment: {
            DURABLE_OBJECT_TOKEN: "delegated-token",
            DURABLE_OBJECT_CONTROL_PLANE_URL: "https://control.example.com"
        },
        async connectWebSocket(url, credential) {
            assert.equal(url, "wss://control.example.com/v1/actors/Counter/one/websocket")
            assert.equal(credential, "delegated-token")
            return {} as ActorConnection
        }
    })
    await client.connect("Counter", "one", {})
})

test("clients reject ambiguous API-key and session-token configuration", async () => {
    const client = new RemoteActorClient(undefined, {
        environment: {
            DURABLE_OBJECT_API_KEY: "backend-key",
            DURABLE_OBJECT_TOKEN: "delegated-token",
            DURABLE_OBJECT_NAMESPACE_ID: "project",
            DURABLE_OBJECT_CONTROL_PLANE_URL: "https://control.example.com"
        },
        async connectWebSocket() {
            return {} as ActorConnection
        }
    })
    await assert.rejects(client.connect("Counter", "one", {}), /exactly one/i)
})

test("target expiry uses real time even when workflow Date.now is frozen", async () => {
    const originalNow = Date.now
    let resolutions = 0
    const usedTokens: string[] = []
    Date.now = () => 1
    try {
        const client = new RemoteActorClient(
            { token: "workflow-token", namespaceId: "project", controlPlaneUrl: "https://control.example.com" },
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
        { token: "workflow-token", namespaceId: "project", controlPlaneUrl: "https://control.example.com" },
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
            { token: "workflow-token", namespaceId: "project", controlPlaneUrl: "https://control.example.com" },
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
        assert.equal(request.url, "/v1/namespaces/project-1/actors/Counter/counter-1/target")
        assert.equal(request.headers.authorization, "Bearer workflow-token")
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
            token: "workflow-token",
            namespaceId: "project-1",
            controlPlaneUrl: `http://127.0.0.1:${port}`
        },
        {
            requestId: () => "00000000-0000-4000-8000-000000000000",
            actorHost: {
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
                namespace_id: "project-1",
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
                namespace_id: "project-1",
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
                namespaceId: "project-1",
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

test("opens actor WebSockets on the control plane without provisioning a host first", async () => {
    const requests: unknown[] = []
    const connection = fakeConnection()
    const client = new RemoteActorClient(
        {
            token: "workflow-token",
            namespaceId: "project-1",
            controlPlaneUrl: "https://control.example.com",
            socketGatewayUrl: "https://sockets.example.com"
        },
        {
            requestId: () => "connection-request",
            connectWebSocket: async (url, token, metadata) => {
                requests.push({ url, token, metadata })
                return connection
            }
        }
    )
    assert.equal(await client.connect("ChatRoom", "room-1", { userId: "user-1" }), connection)
    assert.deepEqual(requests, [
        {
            url: "wss://sockets.example.com/v1/namespaces/project-1/actors/ChatRoom/room-1/websocket",
            token: "workflow-token",
            metadata: { userId: "user-1" }
        }
    ])
})

test("broadcasts to actor sockets without resolving or invoking an actor host", async () => {
    const requests: {
        readonly method?: string
        readonly url?: string
        readonly authorization?: string
        readonly body?: unknown
    }[] = []
    let hostInvocations = 0
    const server = createServer(async (request, response) => {
        requests.push({
            method: request.method,
            url: request.url,
            authorization: request.headers.authorization,
            body: await requestBody(request)
        })
        response.writeHead(204).end()
    })
    const port = await listen(server)
    const client = new RemoteActorClient(
        {
            token: "workflow-token",
            namespaceId: "project-1",
            controlPlaneUrl: "https://control.example.com",
            socketGatewayUrl: `http://127.0.0.1:${port}`
        },
        {
            actorHost: {
                async invoke() {
                    hostInvocations += 1
                    return { type: "completed", result: null, effects: [] }
                }
            }
        }
    )
    try {
        await client.broadcast("ChatRoom", "room-1", { text: "hello" })
        assert.equal(hostInvocations, 0)
        assert.deepEqual(requests, [
            {
                method: "POST",
                url: "/v1/namespaces/project-1/actors/ChatRoom/room-1/socket-effects",
                authorization: "Bearer workflow-token",
                body: {
                    effects: [
                        {
                            type: "broadcast",
                            message: { type: "text", data: JSON.stringify({ text: "hello" }) },
                            except_connection_ids: [],
                            tags: []
                        }
                    ]
                }
            }
        ])
    } finally {
        await close(server)
    }
})

test("forwards actor socket effects to the control-plane gateway after a direct invocation", async () => {
    const requests: { readonly method?: string; readonly url?: string; readonly body?: unknown }[] = []
    const server = createServer(async (request, response) => {
        const body = request.method === "POST" ? await requestBody(request) : undefined
        requests.push({ method: request.method, url: request.url, body })
        if (request.url?.endsWith("/target")) {
            json(response, 200, {
                route: "https://actor.example.com",
                token: "direct-token",
                ownerEpoch: 3,

                expiresAtMs: 4_000_000_000_000
            })
            return
        }
        response.writeHead(204).end()
    })
    const port = await listen(server)
    const client = new RemoteActorClient(
        { token: "workflow-token", namespaceId: "project-1", controlPlaneUrl: `http://127.0.0.1:${port}` },
        {
            requestId: () => "request-1",
            actorHost: {
                async invoke() {
                    return {
                        type: "completed",
                        result: null,
                        effects: [
                            {
                                type: "broadcast",
                                message: { type: "text", data: JSON.stringify({ text: "hello" }) },
                                except_connection_ids: [],
                                tags: []
                            }
                        ]
                    }
                }
            }
        }
    )
    try {
        await client.invoke("ChatRoom", "room-1", "announce", ["hello"])
        assert.deepEqual(requests[1], {
            method: "POST",
            url: "/v1/namespaces/project-1/actors/ChatRoom/room-1/socket-effects",
            body: {
                effects: [
                    {
                        type: "broadcast",
                        message: { type: "text", data: JSON.stringify({ text: "hello" }) },
                        except_connection_ids: [],
                        tags: []
                    }
                ]
            }
        })
    } finally {
        await close(server)
    }
})

test("does not retry a control-plane transport failure", async () => {
    let calls = 0
    const server = createServer(request => {
        calls += 1
        request.socket.destroy()
    })
    const port = await listen(server)
    const client = new RemoteActorClient({
        token: "workflow-token",
        namespaceId: "project-1",
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
        token: "workflow-token",
        namespaceId: "project-1",
        controlPlaneUrl: `http://127.0.0.1:${port}`
    })
    try {
        await assert.rejects(
            client.invoke("Counter", "counter-1", "increment", [2]),
            error => error instanceof ActorInvocationError && error.code === "not_found"
        )
        assert.deepEqual(calls, ["/v1/namespaces/project-1/actors/Counter/counter-1/target"])
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
        token: "workflow-token",
        namespaceId: "project-1",
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
