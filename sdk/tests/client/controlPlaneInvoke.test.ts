import assert from "node:assert/strict"
import { test } from "node:test"

import { RemoteActorClient } from "../../src/client/remoteClient.js"
import { ActorInvocationError, ActorProtocolError } from "../../src/errors.js"

const options = {
    projectId: "project",
    apiKey: "backend-key",
    controlPlaneUrl: "https://control-plane.example.com"
}
const unusedHost = {
    async publish() {
        assert.fail("control-plane invocation must not contact the actor host directly")
    }
}

test("control-plane invocation resolves and invokes in one request", async () => {
    const requests: { url: string; method: string | undefined; body: unknown; authorization: string | null }[] = []
    const client = new RemoteActorClient(options, {
        telemetry: () => {},
        requestId: () => "request-1",
        fetch: async (url, init) => {
            requests.push({
                url: String(url),
                method: init?.method,
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
            url: "https://control-plane.example.com/v1/projects/project/actors/Counter/one/invoke",
            method: "POST",
            body: { requestId: "request-1", method: "increment", args: [2] },
            authorization: "Bearer backend-key"
        }
    ])
})

test("combined invocation preserves placement and does not hide a structured not-found error", async () => {
    let calls = 0
    const client = new RemoteActorClient(
        { ...options, homeRegion: "north-america-west" },
        {
            telemetry: () => {},
            fetch: async (url, init) => {
                calls++
                assert.ok(String(url).endsWith("/invoke"))
                assert.equal(JSON.parse(String(init?.body)).homeRegion, "north-america-west")
                return Response.json(
                    { error: { code: "not_found", message: "actor contract not found" } },
                    { status: 404 }
                )
            },
            actorHost: unusedHost
        }
    )
    for (const actor of ["one", "two"]) {
        await assert.rejects(
            client.invoke("Counter", actor, "increment", []),
            (error: unknown) => error instanceof ActorInvocationError && error.code === "not_found"
        )
    }
    assert.equal(calls, 2)
})

test("invocation replies require an explicit valid outcome", async () => {
    for (const response of [
        ...[{}, { type: "completed" }, { type: "failed", code: 3 }, { type: "reroute" }].map(value =>
            Response.json(value)
        ),
        new Response("unavailable", { status: 502 })
    ]) {
        let calls = 0
        const client = new RemoteActorClient(options, {
            telemetry: () => {},
            fetch: async () => {
                calls++
                return response.clone()
            }
        })
        await assert.rejects(client.invoke("Counter", "one", "increment", []), ActorProtocolError)
        assert.equal(calls, 1)
    }
})
