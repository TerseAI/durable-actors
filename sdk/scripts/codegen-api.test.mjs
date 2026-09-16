import assert from "node:assert/strict"
import { mkdtemp, readFile, readdir, rm } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { test } from "node:test"

test("public codegen returns the same typed artifacts as file generation", async t => {
    const { generateTypeScript } = await import("little-actors/codegen")
    const { generateClient } = await import("../dist/compiler/client-generator.js")
    const directory = await mkdtemp(path.join(os.tmpdir(), "actor-codegen-api-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    const contracts = [
        {
            version: 1,
            actorType: "Room",
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
    assert.deepEqual([...files.keys()].sort(), ["Room.actor.ts", "Room.proxy.ts", "index.ts", "proxy.ts"])
    assert.match(files.get("Room.proxy.ts"), /userId: string/)
    await generateClient(contracts, directory)
    for (const [name, contents] of files) {
        assert.equal(await readFile(path.join(directory, name), "utf8"), contents)
    }
})

test("public codegen supports projects without actors", async () => {
    const { generateTypeScript } = await import("little-actors/codegen")
    const files = await generateTypeScript([])
    assert.deepEqual([...files.keys()].sort(), ["index.ts", "proxy.ts"])
})

test("public contract generation writes the same backend, browser and proxy artifacts", async t => {
    const { generateTypeScript } = await import("little-actors/codegen")
    const { createActorStub } = await import("little-actors/backend")
    const { generateClient } = await import("../dist/compiler/client-generator.js")
    assert.equal(typeof createActorStub, "function")
    const directory = await mkdtemp(path.join(os.tmpdir(), "actor-contract-api-"))
    t.after(() => rm(directory, { recursive: true, force: true }))
    const contract = {
        version: 1,
        actors: [
            {
                actorType: "Room",
                socket: {
                    version: 1,
                    actorType: "Room",
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
    assert.deepEqual([...files.keys()].sort(), [
        "Room.actor.ts",
        "Room.backend.ts",
        "Room.proxy.ts",
        "backend.ts",
        "index.ts",
        "proxy.ts"
    ])
    await generateClient(contract, directory)
    for (const [file, source] of files) assert.equal(await readFile(path.join(directory, file), "utf8"), source)
})
