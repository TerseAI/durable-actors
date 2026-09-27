import assert from "node:assert/strict"
import { once } from "node:events"
import { createServer } from "node:http"
import { test } from "node:test"

import { RemoteActorClient } from "../../src/client/remoteClient.js"
import { ActorInvocationError } from "../../src/errors.js"

test("a mutation with a lost HTTP response is executed once and reports outcome_unknown", async t => {
    let executions = 0
    const host = createServer(async request => {
        request.resume()
        await once(request, "end")
        executions++
        request.socket.destroy()
    })
    t.after(() => host.close())
    host.listen(0, "127.0.0.1")
    await once(host, "listening")
    const address = host.address()
    assert.ok(address && typeof address !== "string")
    const client = new RemoteActorClient(
        { controlPlaneUrl: `http://127.0.0.1:${address.port}` },
        { telemetry: () => {} }
    )
    await assert.rejects(
        client.invoke("Counter", "one", "increment", [1]),
        error => error instanceof ActorInvocationError && error.code === "outcome_unknown"
    )
    assert.equal(executions, 1)
})
