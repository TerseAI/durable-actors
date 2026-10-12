import { fauxAssistantMessage } from "@earendil-works/pi-ai/providers/faux"
import { Database } from "bun:sqlite"
import { expect, test } from "bun:test"
import { mkdtempSync, rmSync } from "node:fs"
import { tmpdir } from "node:os"
import path from "node:path"
import { fileURLToPath } from "node:url"

import { registerActorClass } from "../../../sdk/dist/actor/actor.js"
import { Persistence } from "../../../sdk/dist/actor/schema.js"
import { ActorCompiler } from "../../../sdk/dist/compiler/actor-compiler.js"
import { ActorRuntime } from "../../../sdk/dist/host/actor-runtime.js"
import { SqliteActorDatabase } from "../../../sdk/dist/host/sqlite.js"

import { PiAgent, provider } from "./fixtures/actors.js"

test("the desired API compiles and preserves conversation through actor reconstruction", async () => {
    const entrypoint = fileURLToPath(new URL("./fixtures/actors.ts", import.meta.url))
    const schema = new ActorCompiler().compile(entrypoint)[0]!
    expect(schema.fields).toEqual([{ name: "agent", persistence: Persistence.Ephemeral, visibility: "private" }])
    const definition = registerActorClass(PiAgent, schema)
    const directory = mkdtempSync(path.join(tmpdir(), "pi-actor-"))
    const filename = path.join(directory, "actor.sqlite")
    const seed = new Database(filename)
    seed.exec("CREATE TABLE __terse_fields (name TEXT PRIMARY KEY, value TEXT NOT NULL CHECK(json_valid(value)))")
    seed.close()
    let txid = 1
    provider.setResponses([
        fauxAssistantMessage("Hello Alice"),
        transcript => {
            expect(transcript.messages.some(message => message.role === "user" && message.content === "My name is Alice")).toBe(true)
            return fauxAssistantMessage("Your name is Alice")
        }
    ])
    try {
        for (const [prompt, answer] of [
            ["My name is Alice", "Hello Alice"],
            ["What is my name?", "Your name is Alice"]
        ] as const) {
            const database = new SqliteActorDatabase(
                async () => ++txid,
                () => ++txid
            )
            const runtime = new ActorRuntime(definition, () => {}, database)
            try {
                const reply = await runtime.handle({
                    type: "invoke",
                    request_id: prompt,
                    actor: { project_id: "example", actor_name: "PiAgent", actor_id: "demo" },
                    method: "prompt",
                    args: [prompt],
                    sqlite: { txid, path: filename }
                })
                expect(reply.type).toBe("invoked")
                if (reply.type === "invoked") expect(reply.result).toBe(answer)
                expect(database.fields()).toEqual({})
            } finally {
                runtime.close()
            }
        }
    } finally {
        rmSync(directory, { recursive: true, force: true })
    }
})
