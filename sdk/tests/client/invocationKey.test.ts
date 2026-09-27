import assert from "node:assert/strict"
import { test } from "node:test"

import { HttpActorClient } from "../../src/client-runtime/client.js"
import { createActorInvocationKey } from "../../src/client-runtime/invocation.js"
import { ActorValidationError } from "../../src/errors.js"

test("invocation keys have fresh timestamps and distinct nonces", () => {
    const first = createActorInvocationKey()
    const second = createActorInvocationKey()
    assert.match(first, /^[0-9]+\.[A-Za-z0-9_-]+$/u)
    assert.notEqual(first, second)
    assert.ok(Math.abs(Number(first.split(".")[0]) - Date.now()) < 5000)
})

test("invalid explicit keys fail before discovery or actor execution", async () => {
    const client = new HttpActorClient(
        { controlPlaneUrl: "http://localhost:7100" },
        {
            fetch: async () => assert.fail("invalid key reached discovery")
        }
    )
    for (const idempotencyKey of ["", "no-time", "001.nonce", "1.", "1.invalid nonce", `1.${"x".repeat(254)}`])
        await assert.rejects(client.invoke("Counter", "one", "read", [], { idempotencyKey }), ActorValidationError)
})
