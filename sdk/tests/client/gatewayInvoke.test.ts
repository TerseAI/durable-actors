import assert from "node:assert/strict"
import { test } from "node:test"

import { RemoteActorClient } from "../../src/client/remoteClient.js"
import { ActorInvocationError } from "../../src/errors.js"

const options = {
    projectId: "project",
    apiKey: "backend-key",
    controlPlaneUrl: "https://gateway.example.com",
    invokeThroughGateway: true
}
const unusedHost = {
    async invoke(): Promise<never> {
        return assert.fail("gateway invocation must not contact the actor host directly")
    },
    async publish() {
        assert.fail("gateway invocation must not contact the actor host directly")
    }
}

test("gateway invocation resolves and invokes in one request", async () => {
    const requests: { url: string; body: unknown; authorization: string | null }[] = []
    const client = new RemoteActorClient(options, {
        telemetry: () => {},
        requestId: () => "request-1",
        fetch: async (url, init) => {
            requests.push({
                url: String(url),
                body: JSON.parse(String(init?.body)),
                authorization: new Headers(init?.headers).get("authorization")
            })
            return Response.json({ type: "completed", result: 7 })
        },
        actorHost: unusedHost
    })
    assert.equal(await client.invoke("Counter", "one", "increment", [2]), 7)
    assert.deepEqual(requests, [
        {
            url: "https://gateway.example.com/v1/projects/project/actors/Counter/one/invoke",
            body: { requestId: "request-1", method: "increment", args: [2] },
            authorization: "Bearer backend-key"
        }
    ])
})

test("gateway invocation reports actor failures and unknown outcomes without retrying", async () => {
    for (const [response, code] of [
        [Response.json({ type: "failed", code: "actor_error", message: "boom" }), "actor_error"],
        [Response.json({ error: { code: "outcome_unknown", message: "lost" } }, { status: 502 }), "outcome_unknown"]
    ] as const) {
        let calls = 0
        const client = new RemoteActorClient(options, {
            telemetry: () => {},
            fetch: async () => {
                calls += 1
                return response.clone()
            },
            actorHost: unusedHost
        })
        await assert.rejects(
            client.invoke("Counter", "one", "increment", []),
            (error: unknown) => error instanceof ActorInvocationError && error.code === code
        )
        assert.equal(calls, 1)
    }
})

test("servers without gateway invocation fall back to discovery", async () => {
    const urls: string[] = []
    const client = new RemoteActorClient(options, {
        telemetry: () => {},
        fetch: async url => {
            urls.push(String(url))
            if (String(url).endsWith("/invoke")) return new Response(null, { status: 404 })
            return Response.json({
                route: "https://host.example.com",
                token: "ticket",
                ownerEpoch: 1,
                expiresAtMs: 4_000_000_000_000
            })
        },
        actorHost: {
            async invoke() {
                return { type: "completed", result: 9 }
            },
            async publish() {}
        }
    })
    assert.equal(await client.invoke("Counter", "one", "increment", []), 9)
    assert.equal(await client.invoke("Counter", "two", "increment", []), 9)
    assert.equal(urls.filter(url => url.endsWith("/invoke")).length, 1)
    assert.equal(urls.filter(url => url.endsWith("/find-actor")).length, 2)
})
