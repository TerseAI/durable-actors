import assert from "node:assert/strict"
import { test } from "node:test"

import { ControlPlaneClient } from "../../src/cli/control-plane.js"

const connection = { projectId: "default", controlPlaneUrl: "https://control.example", credential: "admin-key" }

test("connection checks work before any deployment exists", async () => {
    const client = new ControlPlaneClient(connection, async input => {
        if (String(input) === "https://control.example/v1/actors?limit=1")
            return Response.json({ actors: [], nextCursor: null })
        return Response.json({ error: { message: "deployment not found" } }, { status: 404 })
    })
    await client.checkConnection()
})

test("contract and object reads use the active deployment", async () => {
    const requests: string[] = []
    const client = new ControlPlaneClient(connection, async (input, init) => {
        requests.push(String(input))
        assert.equal(init?.method, "GET")
        assert.equal(new Headers(init?.headers).get("authorization"), "Bearer admin-key")
        assert.equal(new Headers(init?.headers).get("content-type"), null)
        assert.equal(init?.body, undefined)
        assert.equal(init?.redirect, "error")
        assert.ok(init?.signal instanceof AbortSignal)
        return Response.json({ received: true })
    })
    assert.deepEqual(await client.getContract("r1"), { received: true })
    await client.listSavedActors(new URLSearchParams({ limit: "50" }))
    await client.inspectActor("Room", "one")
    assert.deepEqual(requests, [
        "https://control.example/v1/projects/default/deployment/contract?revision=r1",
        "https://control.example/v1/actors?limit=50",
        "https://control.example/v1/projects/default/actors/Room/one?include=state"
    ])
})

test("HTTP failures retain their status with JSON, non-JSON, or malformed error documents", async () => {
    for (const [body, message] of [
        [JSON.stringify({ error: { message: "Contract conflict" } }), "Contract conflict"],
        ["upstream unavailable", "Conflict"],
        ["null", "Conflict"],
        [JSON.stringify({ error: { message: {} } }), "Conflict"]
    ]) {
        const client = new ControlPlaneClient(
            connection,
            async () => new Response(body, { status: 409, statusText: "Conflict" })
        )
        await assert.rejects(client.getContract(), { message: `Control-plane request failed (HTTP 409): ${message}` })
    }
})

test("successful responses must contain JSON", async () => {
    const client = new ControlPlaneClient(connection, async () => new Response("not JSON"))
    await assert.rejects(client.getContract(), /invalid JSON \(HTTP 200\)/u)
})

test("transport failures do not retry writes and warn that their outcome is unknown", async () => {
    let requests = 0
    const client = new ControlPlaneClient(connection, async () => {
        requests++
        throw new TypeError("fetch failed")
    })
    await assert.rejects(
        client.registerDeployment({ codeRevision: "r1" }),
        /Cannot complete PUT.*may have reached the server/u
    )
    assert.equal(requests, 1)
    await assert.rejects(client.getContract(), error => {
        assert.match((error as Error).message, /Cannot complete GET/u)
        assert.doesNotMatch((error as Error).message, /may have reached|admin-key/u)
        return true
    })
    assert.equal(requests, 2)
})

test("live inventory streams carry server-side credentials and cancellation", async () => {
    const controller = new AbortController()
    const client = new ControlPlaneClient(connection, async (url, options) => {
        assert.equal(url, "https://control.example/v1/observe/events")
        assert.equal(new Headers(options?.headers).get("authorization"), "Bearer admin-key")
        assert.equal(new Headers(options?.headers).get("accept"), "text/event-stream")
        assert.equal(options?.signal, controller.signal)
        assert.equal(options?.redirect, "error")
        return new Response("event: inventory\ndata: {}\n\n", { headers: { "content-type": "text/event-stream" } })
    })
    assert.match(await (await client.openActorStream(controller.signal)).text(), /event: inventory/u)
})

test("live inventory rejects denied responses and non-streaming upstreams", async () => {
    for (const response of [new Response("secret", { status: 403 }), Response.json({})]) {
        const client = new ControlPlaneClient(connection, async () => response)
        await assert.rejects(client.openActorStream(new AbortController().signal), {
            message: "Live inventory is unavailable"
        })
    }
})

test("request traces stream through the authenticated control-plane client", async () => {
    const controller = new AbortController()
    const client = new ControlPlaneClient(connection, async (url, options) => {
        assert.equal(url, "https://control.example/v1/observe/requests/events")
        assert.equal(new Headers(options?.headers).get("authorization"), "Bearer admin-key")
        assert.equal(options?.signal, controller.signal)
        return new Response("event: requests\ndata: {}\n\n", { headers: { "content-type": "text/event-stream" } })
    })
    assert.match(await (await client.openRequestStream(controller.signal)).text(), /event: requests/u)
})

test("SQL queries are posted as JSON with server-side credentials", async () => {
    const query = { sql: "SELECT COUNT(*) AS total FROM request_events WHERE actor_id = ?", params: ["one"] }
    const client = new ControlPlaneClient(connection, async (url, options) => {
        assert.equal(url, "https://control.example/v1/observe/query")
        assert.equal(options?.method, "POST")
        assert.equal(new Headers(options?.headers).get("authorization"), "Bearer admin-key")
        assert.deepEqual(JSON.parse(String(options?.body)), query)
        return Response.json({ rows: [{ total: 2 }], truncated: false })
    })
    assert.deepEqual(await client.query(query), { rows: [{ total: 2 }], truncated: false })
})
