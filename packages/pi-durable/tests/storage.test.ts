import { BACKGROUND_CONTEXT as context } from "@earendil-works/chord/context"
import { createModels } from "@earendil-works/pi-ai/models"
import { fauxAssistantMessage, fauxProvider } from "@earendil-works/pi-ai/providers/faux"
import { Harness, createRegistry } from "@earendil-works/pi-durable"
import { Database } from "bun:sqlite"
import { expect, test } from "bun:test"
import type { ActorDatabase, SqliteValue } from "durable-actors"

import { ActorStorage } from "../src/storage.js"

class TestDatabase implements ActorDatabase {
    constructor(private readonly database: Database) {}

    exec<Row extends object>(sql: string, ...bindings: SqliteValue[]): Row[] {
        return this.database.query(sql).all(...bindings) as Row[]
    }
}

test("reopening the actor database restores the root conversation and transcript", async () => {
    const database = new Database(":memory:")
    const actorDatabase = new TestDatabase(database)
    const options = { models: createModels(), registry: createRegistry() }
    try {
        const first = await Harness.open(new ActorStorage(actorDatabase), options, context)
        const root = await first.root(context)
        await (await root.submit({ type: "write", entry: { kind: "example.note", data: "Remember Alice" } }, context)).wait(context)
        await first.close(context)

        const second = await Harness.open(new ActorStorage(actorDatabase), options, context)
        try {
            const restored = await second.root(context)
            const view = await restored.viewState(context)
            expect(restored.id).toBe(root.id)
            expect(view.value.entries.some(entry => entry.kind === "example.note" && entry.data === "Remember Alice")).toBe(true)
            view.dispose()
            await (await restored.submit({ type: "write", entry: { kind: "example.note", data: "Hello again" } }, context)).wait(context)
        } finally {
            await second.close(context)
        }
    } finally {
        database.close()
    }
})

test("a failed database write leaves the in-memory commit sequence unchanged", async () => {
    const database = new Database(":memory:")
    const storage = new ActorStorage(new TestDatabase(database))
    try {
        database.exec("CREATE TRIGGER reject_commit BEFORE INSERT ON pi_commits BEGIN SELECT RAISE(ABORT, 'disk failure'); END")
        await expect(storage.commit([], context)).rejects.toThrow("disk failure")
        database.exec("DROP TRIGGER reject_commit")
        expect(Number(await storage.commit([], context))).toBe(1)
    } finally {
        await storage.close(context)
        database.close()
    }
})

test("a new prompt sees the conversation's previous model exchange after reopening", async () => {
    const database = new Database(":memory:")
    const actorDatabase = new TestDatabase(database)
    const faux = fauxProvider({ tokensPerSecond: 0 })
    const models = createModels()
    models.setProvider(faux.provider)
    const options = { models, registry: createRegistry() }
    const model = faux.getModel()
    faux.setResponses([
        fauxAssistantMessage("Hello Alice"),
        transcript => {
            expect(transcript.messages.some(message => message.role === "user" && message.content === "My name is Alice")).toBe(true)
            expect(transcript.messages.some(message => message.role === "assistant" && message.content.some(part => part.type === "text" && part.text === "Hello Alice"))).toBe(true)
            return fauxAssistantMessage("Your name is Alice")
        }
    ])
    try {
        for (const text of ["My name is Alice", "What is my name?"]) {
            const harness = await Harness.open(new ActorStorage(actorDatabase), options, context)
            try {
                const root = await harness.root(context, { agent: { model: { provider: model.provider, modelId: model.id } } })
                const settled = await (await root.submit({ type: "input", content: text }, context)).wait(context)
                expect(settled.status).toBe("done")
            } finally {
                await harness.close(context)
            }
        }
    } finally {
        database.close()
    }
})
