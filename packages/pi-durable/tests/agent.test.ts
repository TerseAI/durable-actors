import { Type, createModels } from "@earendil-works/pi-ai"
import { fauxAssistantMessage, fauxProvider, fauxToolCall } from "@earendil-works/pi-ai/providers/faux"
import { createRegistry, defineDoc, defineExtension, defineTool } from "@earendil-works/pi-durable"
import { Database } from "bun:sqlite"
import { expect, test } from "bun:test"
import type { Actor, ActorDatabase, SqliteValue } from "durable-actors"

import { DurablePiAgent } from "../src/index.js"

class DatabaseActor implements Pick<Actor, "db"> {
    reads = 0
    constructor(private readonly database: Database) {}

    get db(): ActorDatabase {
        this.reads++
        return {
            exec: <Row extends object>(sql: string, ...bindings: SqliteValue[]) => this.database.query(sql).all(...bindings) as Row[]
        }
    }
}

function options() {
    const provider = fauxProvider({ tokensPerSecond: 0 })
    const models = createModels()
    models.setProvider(provider.provider)
    const model = provider.getModel()
    const registry = createRegistry()
    return { provider, config: { models, registry, model: { provider: model.provider, modelId: model.id } } }
}

test("the agent opens storage lazily and returns a text result", async () => {
    const database = new Database(":memory:")
    try {
        const actor = new DatabaseActor(database)
        const { provider, config } = options()
        const agent = new DurablePiAgent(actor, config)
        expect(actor.reads).toBe(0)
        provider.setResponses([fauxAssistantMessage("Hello Alice")])
        expect(await agent.prompt("Hello")).toEqual({ text: "Hello Alice" })
        expect(actor.reads).toBe(1)
    } finally {
        database.close()
    }
})

test("the same agent continues the persisted conversation across prompts", async () => {
    const database = new Database(":memory:")
    const { provider, config } = options()
    provider.setResponses([
        fauxAssistantMessage("Hello Alice"),
        transcript => {
            expect(transcript.messages.some(message => message.role === "user" && message.content === "My name is Alice")).toBe(true)
            return fauxAssistantMessage("Your name is Alice")
        }
    ])
    try {
        const agent = new DurablePiAgent(new DatabaseActor(database), config)
        await agent.prompt("My name is Alice")
        expect(await agent.prompt("What is my name?")).toEqual({ text: "Your name is Alice" })
    } finally {
        database.close()
    }
})

test("the agent uses tools from the supplied registry", async () => {
    const database = new Database(":memory:")
    const { provider, config } = options()
    let calls = 0
    config.registry.install(
        defineExtension({
            name: "example",
            tools: [
                defineTool({
                    name: "greet",
                    description: "Greet the user",
                    parameters: Type.Object({}),
                    execute: async () => {
                        calls++
                        return { content: [{ type: "text", text: "Hello Alice" }] }
                    }
                })
            ]
        })
    )
    provider.setResponses([
        fauxAssistantMessage(fauxToolCall("greet", {}), { stopReason: "toolUse" }),
        transcript => {
            expect(transcript.messages.some(message => message.role === "toolResult" && message.content.some(part => part.type === "text" && part.text === "Hello Alice"))).toBe(true)
            return fauxAssistantMessage("Hello Alice")
        }
    ])
    try {
        const agent = new DurablePiAgent(new DatabaseActor(database), config)
        expect(await agent.prompt("Greet me")).toEqual({ text: "Hello Alice" })
        expect(calls).toBe(1)
    } finally {
        database.close()
    }
})

test("the exposed session gives access to native Pi APIs and persists their changes", async () => {
    const database = new Database(":memory:")
    const { config } = options()
    const Integration = defineDoc<{ connected: boolean }>({
        kind: "example.integration",
        version: 1,
        scope: "conversation",
        history: "latest",
        fork: "initial",
        initial: () => ({ connected: false })
    })
    try {
        const actor = new DatabaseActor(database)
        const agent = new DurablePiAgent(actor, config)
        expect(actor.reads).toBe(0)
        await agent.session.run(async ({ harness, conversation, context }) => {
            expect((await harness.root(context)).id).toBe(conversation.id)
            await conversation.commit(async tx => {
                const integration = await tx.doc(Integration, conversation.id)
                integration.connected = true
            }, context)
        })

        const restored = new DurablePiAgent(actor, config)
        const integration = await restored.session.run(({ harness, conversation, context }) => harness.snapshot(Integration, conversation.id, context))
        expect(integration).toEqual({ connected: true })
    } finally {
        database.close()
    }
})

test("native harness options reach Pi without adapter-specific equivalents", async () => {
    const database = new Database(":memory:")
    const { config } = options()
    let created = 0
    try {
        const agent = new DurablePiAgent(new DatabaseActor(database), {
            ...config,
            conversationCreated: async () => {
                created++
            }
        })
        await agent.session.run(() => "ready")
        expect(created).toBe(1)
    } finally {
        database.close()
    }
})
