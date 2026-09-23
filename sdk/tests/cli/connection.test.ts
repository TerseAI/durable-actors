import assert from "node:assert/strict"
import { test } from "node:test"

import { connection } from "../../src/cli/connection.js"
import { createControlPlaneClient } from "../../src/cli/control-plane.js"

test("local CLI connections default to project local without credentials", () => {
    const settings = connection({})
    assert.equal(settings.projectId, "local")
    assert.equal(settings.controlPlaneUrl, "http://127.0.0.1:7100")
    assert.equal(settings.credential, undefined)
})

test("local CLI requests and event streams omit authorization without a secret", async () => {
    const paths: string[] = []
    const client = createControlPlaneClient({}, async (url, init) => {
        paths.push(new URL(String(url)).pathname)
        assert.equal(new Headers(init?.headers).get("authorization"), null)
        return paths.at(-1)!.endsWith("events")
            ? new Response("data: {}\n\n", { headers: { "content-type": "text/event-stream" } })
            : Response.json({})
    })
    await client.getContract()
    await client.registerDeployment({})
    await client.checkConnection()
    await client.openActorStream(new AbortController().signal)
    await client.openRequestStream(new AbortController().signal)
    assert.deepEqual(paths, [
        "/v1/projects/local/deployment/contract",
        "/v1/projects/local/deployment",
        "/v1/projects/local/observe/actors",
        "/v1/projects/local/observe/events",
        "/v1/projects/local/observe/requests/events"
    ])
})

test("local CLI connections honor configured projects and secrets including the API key alias", () => {
    for (const key of ["DURABLE_ACTORS_SECRET", "DURABLE_ACTORS_API_KEY"])
        assert.equal(connection({ [key]: "my-secret" }).credential, "my-secret")
    assert.equal(connection({ DURABLE_ACTORS_PROJECT_ID: "custom" }).projectId, "custom")
})
