import assert from "node:assert/strict"
import { test } from "node:test"

import { ActorDirectoryClient } from "../../src/client-runtime/directory.js"

const objectId = "00000000-0000-4000-8000-000000000001"
const options = { controlPlaneUrl: "https://actors.test", apiKey: "secret", projectId: "project" }
const record = {
    objectId,
    actor: { project_id: "project", actor_name: "Counter", actor_id: "one" },
    firstIngressRegion: "north-america-west",
    homeRegion: "north-america-west"
}

test("resolving a name and looking up its global ID use creation and read-only operations respectively", async () => {
    const calls: { url: string; method: string | undefined }[] = []
    const client = new ActorDirectoryClient(options, async (url, init) => {
        calls.push({ url: String(url), method: init?.method })
        assert.equal(new Headers(init?.headers).get("authorization"), "Bearer secret")
        return Response.json(record)
    })
    assert.deepEqual(await client.resolve("Counter", "one"), {
        objectId,
        projectId: "project",
        actorName: "Counter",
        actorId: "one",
        homeRegion: "north-america-west",
        firstIngressRegion: "north-america-west"
    })
    assert.equal((await client.get(objectId))?.homeRegion, "north-america-west")
    assert.deepEqual(calls, [
        { url: "https://actors.test/v1/projects/project/actors/Counter/one/resolve", method: "POST" },
        { url: `https://actors.test/v1/projects/project/objects/${objectId}`, method: "GET" }
    ])
})

test("missing IDs do not create actors and a response cannot cross project boundaries", async () => {
    const missing = new ActorDirectoryClient(options, async () => new Response("", { status: 404 }))
    assert.equal(await missing.get(objectId), null)
    const other = new ActorDirectoryClient(options, async () =>
        Response.json({
            ...record,
            actor: { ...record.actor, project_id: "other" }
        })
    )
    await assert.rejects(other.get(objectId), /scope/u)
})
