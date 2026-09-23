import assert from "node:assert/strict"
import { setTimeout } from "node:timers/promises"

const { RemoteActorClient } = await import(process.argv[2])
let hostUrl
const client = new RemoteActorClient(undefined, {
    fetch: async (url, init) => {
        if (String(url).endsWith("/invoke")) hostUrl = String(url)
        return fetch(url, init)
    }
})
assert.equal(await client.invoke("Counter", "one", "increment", []), 1)
let previous = await client.invoke("Counter", "one", "processId", [])
for (const [method, expected] of [["read", 1], ["increment", 2]]) {
    await setTimeout(2_000)
    await assert.rejects(fetch(hostUrl), error => error.cause?.code === "ECONNREFUSED")
    assert.equal(await client.invoke("Counter", "one", method, []), expected)
    const current = await client.invoke("Counter", "one", "processId", [])
    assert.notEqual(current, previous, "an idle local host should be replaced")
    previous = current
}
