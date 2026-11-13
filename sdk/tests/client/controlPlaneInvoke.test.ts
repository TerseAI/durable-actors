import assert from "node:assert/strict"
import { test } from "node:test"

import { ActorValidationError } from "../../src/client-runtime/errors.js"
import { RemoteActorClient } from "../../src/client/remoteClient.js"

const options = { projectId: "project", apiKey: "backend-key", controlPlaneUrl: "https://gateway.example" }

test("every invocation uses the configured gateway and caller credential", async () => {
    const requests: { url: string; body: unknown }[] = []
    const client = new RemoteActorClient(options, {
        requestId: () => "request-1",
        fetch: async (url, init) => {
            assert.equal(new Headers(init?.headers).get("authorization"), "Bearer backend-key")
            requests.push({ url: String(url), body: JSON.parse(String(init?.body)) })
            return Response.json({ outcome: { type: "completed", result: 7 } })
        }
    })
    for (const amount of [2, 3]) assert.equal(await client.invoke("Counter", "one", "increment", [amount]), 7)
    assert.deepEqual(
        requests,
        [2, 3].map(amount => ({
            url: "https://gateway.example/v1/projects/project/actors/Counter/one/invoke",
            body: { requestId: "request-1", method: "increment", args: [amount] }
        }))
    )
})

test("only explicit pre-execution rejection retries with the same request ID", async () => {
    const requests: unknown[] = []
    const client = new RemoteActorClient(options, {
        requestId: () => "request-1",
        fetch: async (_url, init) => {
            requests.push(JSON.parse(String(init?.body)))
            return Response.json({
                outcome:
                    requests.length === 1
                        ? { type: "not_executed", reason: "host_unavailable" }
                        : { type: "completed", result: 7 }
            })
        }
    })
    assert.equal(await client.invoke("Counter", "one", "increment", []), 7)
    assert.equal(requests.length, 2)
    assert.deepEqual(requests[0], requests[1])
})

test("warm sends the duration, credential and homeRegion and resolves once the host is warm", async () => {
    const requests: { url: string; body: unknown }[] = []
    const client = new RemoteActorClient(
        { ...options, homeRegion: "north-america-east" },
        {
            requestId: () => "request-1",
            fetch: async (url, init) => {
                assert.equal(new Headers(init?.headers).get("authorization"), "Bearer backend-key")
                requests.push({ url: String(url), body: JSON.parse(String(init?.body)) })
                return Response.json({ status: "OK" })
            }
        }
    )
    assert.deepEqual(await client.warm("Counter", "one", 30_000), { status: "OK" })
    assert.deepEqual(requests, [
        {
            url: "https://gateway.example/v1/projects/project/actors/Counter/one/warm-host",
            body: { requestId: "request-1", warmMs: 30_000, homeRegion: "north-america-east" }
        }
    ])
})

test("warm sends waitForCompletion false and timeoutMs and resolves on 202", async () => {
    const bodies: unknown[] = []
    const client = new RemoteActorClient(options, {
        requestId: () => "request-1",
        fetch: async (_url, init) => {
            bodies.push(JSON.parse(String(init?.body)))
            return Response.json({ status: "accepted" }, { status: 202 })
        }
    })
    assert.deepEqual(await client.warm("Counter", "one", 30_000, { waitForCompletion: false }), {
        status: "accepted"
    })
    assert.deepEqual(await client.warm("Counter", "one", 30_000, { waitForCompletion: false, timeoutMs: 2000 }), {
        status: "accepted"
    })
    assert.deepEqual(bodies, [
        { requestId: "request-1", warmMs: 30_000, waitForCompletion: false },
        { requestId: "request-1", warmMs: 30_000, waitForCompletion: false, timeoutMs: 2000 }
    ])
})

test("warm accepts 0 to 86400000 and rejects anything else before sending", async () => {
    let sent = 0
    const client = new RemoteActorClient(options, {
        fetch: async () => {
            sent++
            return Response.json({ status: "OK" })
        }
    })
    for (const warmMs of [-5, 1.5, NaN, 86_400_001])
        await assert.rejects(client.warm("Counter", "one", warmMs), ActorValidationError)
    assert.equal(sent, 0)
    for (const warmMs of [0, 86_400_000]) await client.warm("Counter", "one", warmMs)
    assert.equal(sent, 2)
})
