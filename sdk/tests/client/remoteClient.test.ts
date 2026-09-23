import assert from "node:assert/strict"
import { createServer } from "node:http"
import type { Server, ServerResponse } from "node:http"
import { performance } from "node:perf_hooks"
import { test } from "node:test"

import type { ActorConnection } from "../../src/actor/socket.js"
import { RemoteActorClient } from "../../src/client/remoteClient.js"
import { ActorInvocationError, ActorProtocolError } from "../../src/errors.js"

test("invalid discovery epochs and deadlines fail before host dispatch", async () => {
    for (const field of ["ownerEpoch", "expiresAtMs"]) {
        for (const value of [0, -1, 1.5, "1", null, Number.MAX_SAFE_INTEGER + 1]) {
            const client = new RemoteActorClient(undefined, {
                environment: {},
                telemetry: () => {},
                fetch: async () =>
                    Response.json({
                        route: "https://host.example.com",
                        token: "ticket",
                        ownerEpoch: 1,
                        expiresAtMs: 4_000_000_000_000,
                        [field]: value
                    }),
                actorHost: {
                    async invoke() {
                        return assert.fail("invalid target reached actor host")
                    },
                    async publish() {
                        assert.fail("invalid target reached actor host")
                    }
                }
            })
            await assert.rejects(client.invoke("Counter", "one", "increment", []), ActorProtocolError)
        }
    }
})

for (const local of [false, true]) {
    test(`${local ? "Unauthenticated local" : "API-key"} clients invoke, connect, and broadcast`, async () => {
        const requests: string[] = []
        const client = new RemoteActorClient(undefined, {
            environment: local
                ? {}
                : {
                      DURABLE_ACTORS_PROJECT_ID: "default",
                      DURABLE_ACTORS_SECRET: "backend-key",
                      DURABLE_ACTORS_CONTROL_PLANE_URL: "https://control.example.com"
                  },
            telemetry: () => {},
            fetch: async (url, options) => {
                requests.push(String(url))
                assert.equal(new Headers(options?.headers).get("authorization"), local ? null : "Bearer backend-key")
                const socket = String(url).endsWith("/find-websocket")
                assert.deepEqual(JSON.parse(String(options?.body)), socket ? { metadata: {} } : {})
                if (socket)
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
                    return { type: "completed", result: 7 }
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
        const origin = local ? "http://127.0.0.1:7100" : "https://control.example.com"
        const project = local ? "local" : "default"
        assert.deepEqual(requests, [
            `${origin}/v1/projects/${project}/actors/Counter/one/find-actor`,
            `${origin}/v1/projects/${project}/actors/Counter/one/find-websocket`
        ])
    })
}

test("target expiry uses real time even when workflow Date.now is frozen", async () => {
    const originalNow = Date.now
    let resolutions = 0
    const usedTokens: string[] = []
    Date.now = () => 1
    try {
        const client = new RemoteActorClient(
            { projectId: "default", apiKey: "backend-key", controlPlaneUrl: "https://control.example.com" },
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
                        return { type: "completed", result: null }
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
        { projectId: "default", apiKey: "backend-key", controlPlaneUrl: "https://control.example.com" },
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
                    return { type: "completed", result: 7 }
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
            { projectId: "default", apiKey: "backend-key", controlPlaneUrl: "https://control.example.com" },
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
        assert.equal(request.url, "/v1/projects/default/actors/Counter/counter-1/find-actor")
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
            projectId: "default",
            apiKey: "backend-key",
            controlPlaneUrl: `http://127.0.0.1:${port}`
        },
        {
            requestId: () => "00000000-0000-4000-8000-000000000000",
            actorHost: {
                async publish() {},
                async invoke(target, invocation) {
                    hostInvocations.push({ target, invocation })
                    return { type: "completed", result: 7 }
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
                actor_name: "Counter",
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
                actor_name: "Counter",
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
                projectId: "default",
                requestId: "00000000-0000-4000-8000-000000000000",
                actorName: "Counter",
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
            projectId: "default",
            apiKey: "backend-key",
            controlPlaneUrl: "https://control.example.com"
        },
        {
            requestId: () => "connection-request",
            fetch: async (url, init) => {
                assert.equal(
                    String(url),
                    "https://control.example.com/v1/projects/default/actors/ChatRoom/room-1/find-websocket"
                )
                assert.equal(JSON.parse(init!.body as string).backend, undefined)
                assert.deepEqual(JSON.parse(init!.body as string).metadata, { userId: "user-1" })
                return Response.json({
                    websocketUrl: "wss://host.modal.test/v1/socket?key=host-ticket",
                    key: "host-ticket"
                })
            },
            connectWebSocket: async url => {
                requests.push({ url })
                return connection
            }
        }
    )
    assert.equal(await client.connect("ChatRoom", "room-1", { userId: "user-1" }), connection)
    assert.deepEqual(requests, [
        {
            url: "wss://host.modal.test/v1/socket?key=host-ticket"
        }
    ])
})

test("does not retry a control-plane transport failure", async () => {
    let calls = 0
    const server = createServer(request => {
        calls += 1
        request.socket.destroy()
    })
    const port = await listen(server)
    const client = new RemoteActorClient({
        projectId: "default",
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
        projectId: "default",
        apiKey: "backend-key",
        controlPlaneUrl: `http://127.0.0.1:${port}`
    })
    try {
        await assert.rejects(
            client.invoke("Counter", "counter-1", "increment", [2]),
            error => error instanceof ActorInvocationError && error.code === "not_found"
        )
        assert.deepEqual(calls, ["/v1/projects/default/actors/Counter/counter-1/find-actor"])
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
        projectId: "default",
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

test("broadcasts use the owning host HTTP transport", async () => {
    let published = false
    const client = new RemoteActorClient(
        { projectId: "default", apiKey: "backend-key", controlPlaneUrl: "https://control.example" },
        {
            telemetry: () => {},
            fetch: async (url, options) => {
                assert.equal(String(url), "https://control.example/v1/projects/default/actors/Room/lobby/find-actor")
                assert.deepEqual(JSON.parse(String(options?.body)), {})
                return Response.json({
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

test("project clients retain project identity across resolution, RPC, and broadcast", async () => {
    const requests: string[] = []
    const invoked: string[] = []
    const published: string[] = []
    for (const projectId of ["team-a", "team-b"]) {
        const client = new RemoteActorClient(
            { projectId, apiKey: "key", controlPlaneUrl: "https://control.example" },
            {
                telemetry: () => {},
                fetch: async url => {
                    requests.push(String(url))
                    return Response.json({
                        route: "https://host.example",
                        token: "ticket",
                        ownerEpoch: 1,
                        expiresAtMs: 4_000_000_000_000
                    })
                },
                actorHost: {
                    async invoke(_target, invocation) {
                        invoked.push(invocation.projectId!)
                        return { type: "completed", result: projectId }
                    },
                    async publish(_target, actor) {
                        published.push(actor.projectId!)
                    }
                }
            }
        )
        assert.equal(await client.invoke("Counter", "same", "increment", []), projectId)
        await client.broadcast("Counter", "same", "updated")
    }
    assert.deepEqual(
        requests,
        ["team-a", "team-b"].map(
            project => `https://control.example/v1/projects/${project}/actors/Counter/same/find-actor`
        )
    )
    assert.deepEqual(invoked, ["team-a", "team-b"])
    assert.deepEqual(published, ["team-a", "team-b"])
})
