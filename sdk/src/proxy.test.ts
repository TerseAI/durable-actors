import assert from "node:assert/strict"
import { test } from "node:test"

import { SocketProxy } from "./proxy.js"

const actors = { Room: {} }

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
                    key: "socket-ticket"
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
        key: "socket-ticket"
    })
    assert.equal(requests[0]!.headers.get("authorization"), "Bearer backend-secret")
    assert.equal(requests[0]!.url, "https://actors.example.com/v1/actors/Room/lobby/socket-ticket")
    assert.deepEqual(requests[0]!.body, {
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
