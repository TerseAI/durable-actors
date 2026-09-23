import assert from "node:assert/strict"
import { setTimeout } from "node:timers/promises"

const { RemoteActorClient } = await import(process.argv[2])
const first = await new RemoteActorClient().invoke("Counter", "one", "processId", [])
await setTimeout(2_000)
const second = await new RemoteActorClient().invoke("Counter", "one", "processId", [])
assert.notEqual(second, first, "an idle local host should be replaced")
