import assert from "node:assert/strict"
import { mkdtemp, readFile, readdir, rm } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { test } from "node:test"

test("public codegen returns the same typed artifacts as file generation", async t => {
    const { generateTypeScript } = await import("durable-actors/codegen")
    const { generateClient } = await import("../../dist/compiler/generators/client-generator.js")
    const directory = await mkdtemp(path.join(os.tmpdir(), "actor-codegen-api-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    const contracts = [
        {
            version: 1,
            actorName: "Room",
            emittable: [],
            schema: {
                definitions: {
                    Metadata: { type: "object", properties: { userId: { type: "string" } }, required: ["userId"] },
                    Incoming: { type: "string" },
                    Outgoing: false,
                    State: { type: "object" }
                }
            }
        }
    ]

    const files = await generateTypeScript(contracts)
    assert.deepEqual(await readdir(directory), [])
    assert.ok(files.has("runtime/client.ts"))
    assert.ok(files.has("runtime/index.ts"))
    assert.match(files.get("index.ts"), /userId: string/)
    await generateClient(contracts, directory)
    for (const [name, contents] of files) {
        assert.equal(await readFile(path.join(directory, name), "utf8"), contents)
    }
})

test("public codegen supports projects without actors", async () => {
    const { generateTypeScript } = await import("durable-actors/codegen")
    const files = await generateTypeScript([])
    assert.ok(files.has("runtime/client.ts"))
    assert.ok(files.has("runtime/index.ts"))
})

test("public contract generation writes a backend module and a self-contained runtime", async t => {
    const { generateTypeScript } = await import("durable-actors/codegen")
    const { createActorStub } = await import("durable-actors/backend")
    const { generateClient } = await import("../../dist/compiler/generators/client-generator.js")
    assert.equal(typeof createActorStub, "function")
    const directory = await mkdtemp(path.join(os.tmpdir(), "actor-contract-api-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    const contract = {
        version: 1,
        actors: [
            {
                actorName: "Room",
                socket: {
                    version: 1,
                    actorName: "Room",
                    emittable: [],
                    schema: {
                        definitions: {
                            Metadata: { type: "object" },
                            Incoming: false,
                            Outgoing: false,
                            State: { type: "object" }
                        }
                    }
                },
                rpc: {
                    schema: { definitions: {} },
                    methods: [{ name: "clear", parameters: [], result: { kind: "void" } }]
                }
            }
        ]
    }
    const files = await generateTypeScript(contract)
    assert.ok(files.has("runtime/client.ts"))
    assert.ok(files.has("runtime/index.ts"))
    assert.match(files.get("index.ts"), /export const actors/)
    assert.doesNotMatch(files.get("index.ts"), /export const clients|createClient/)
    assert.match(files.get("index.ts"), /export class ActorProxy/)
    await generateClient(contract, directory)
    assert.deepEqual((await readdir(directory, { recursive: true })).filter(file => file !== "runtime").sort(), [...files.keys()].sort())
    for (const [file, source] of files) assert.equal(await readFile(path.join(directory, file), "utf8"), source)
})
