import assert from "node:assert/strict"
import { test } from "node:test"

import { SocketProxy } from "./proxy.js"

const actors = { Room: {} }

test("socket setup accepts an explicit home region and validates its timeout", async () => {
    const proxy = new SocketProxy(
        actors,
        { apiKey: "secret", setupTimeoutMs: 180000 },
        {
            fetch: async (_url, init) => {
                assert.equal(JSON.parse(init!.body as string).homeRegion, "north-america-west")
                return Response.json({
                    websocketUrl: "wss://modal.example/v1/socket?_modal_connect_token=modal&key=actor",
                    transport: "websocket",
                    homeRegion: "north-america-east",
                    connectByMs: 1000,
                    authorizedUntilMs: 900000
                })
            }
        }
    )
    const grant = await proxy.handle({
        actorType: "Room",
        actorId: "room",
        metadata: {},
        homeRegion: "north-america-west"
    })
    assert.equal(new URL(grant.websocketUrl).searchParams.get("_modal_connect_token"), "modal")
    assert.throws(() => new SocketProxy(actors, { apiKey: "secret", setupTimeoutMs: 0 }), /timeout/i)
})

test("proxy issues socket authorization using only server-selected target and metadata", async () => {
    const requests: { url: string; headers: Headers; body: unknown }[] = []
    const proxy = new SocketProxy(
        actors,
        { controlPlaneUrl: "https://actors.example.com", apiKey: "backend-secret" },
        {
            fetch: async (url, init) => {
                requests.push({
                    url: String(url),
                    headers: new Headers(init?.headers),
                    body: JSON.parse(init!.body as string)
                })
                return Response.json({
                    websocketUrl: "wss://actors.example.com/v1/socket?key=socket-ticket",
                    transport: "websocket",
                    homeRegion: "north-america-east",
                    connectByMs: 1000,
                    authorizedUntilMs: 900000
                })
            }
        }
    )
    const grant = await proxy.handle({
        actorType: "Room",
        actorId: "lobby",
        metadata: { userId: "trusted" }
    })
    assert.deepEqual(grant, {
        websocketUrl: "wss://actors.example.com/v1/socket?key=socket-ticket",
        transport: "websocket",
        homeRegion: "north-america-east",
        connectByMs: 1000,
        authorizedUntilMs: 900000
    })
    assert.equal(requests[0]!.headers.get("authorization"), "Bearer backend-secret")
    assert.equal(requests[0]!.url, "https://actors.example.com/v1/actors/Room/lobby/connect")
    assert.deepEqual(requests[0]!.body, {
        transport: "websocket",
        metadata: { userId: "trusted" },
        authorizationLifetimeMs: 900000
    })
    assert.equal(requests.length, 1)
})

test("proxy requires JSON metadata and a backend API key", async () => {
    const proxy = new SocketProxy(
        actors,
        { controlPlaneUrl: "https://actors.example.com", apiKey: "secret" },
        {
            fetch: async () => assert.fail("invalid request reached issuance")
        }
    )
    await assert.rejects(
        proxy.handle({ actorType: "Room", actorId: "lobby", metadata: { invalid: () => undefined } }),
        /JSON/
    )
    assert.throws(
        () => new SocketProxy(actors, { controlPlaneUrl: "https://actors.example.com", apiKey: "" }),
        /API key/
    )
})
