import assert from "node:assert/strict"
import { test } from "node:test"

import { HttpActorHostTransport } from "../../src/client-runtime/http.js"

const target = { route: "https://host.example", token: "ticket", ownerEpoch: 3, expiresAtMs: 4000000000000 }
const invocation = {
    projectId: "team",
    actorName: "Counter",
    actorId: "one",
    requestId: "request-1",
    method: "increment",
    args: [2]
}

test("socket broadcasts use actor-scoped HTTP with the same ownership ticket", async () => {
    const effects = [
        { type: "broadcast", message: { type: "text", data: "hello" }, except_connection_ids: [], tags: [] }
    ]
    const transport = new HttpActorHostTransport(async (url, init) => {
        assert.equal(url, "https://host.example/v1/projects/team/actors/Counter/one/socket-effects")
        assert.equal(new Headers(init?.headers).get("authorization"), "Bearer ticket")
        assert.deepEqual(JSON.parse(String(init?.body)), { ownerEpoch: 3, effects })
        return new Response(null, { status: 204 })
    })
    await transport.publish(target, invocation, effects)
})
