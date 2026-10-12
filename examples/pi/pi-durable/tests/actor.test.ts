import { fauxAssistantMessage, fauxProvider } from "@earendil-works/pi-ai/providers/faux"
import { Database } from "bun:sqlite"
import { expect, mock, test } from "bun:test"
import type { ActorDatabase, ActorSocket, SqliteValue } from "durable-actors"

import { constructWithActorDatabase, runWithActorDatabase } from "../../../../sdk/dist/actor/database.js"

const provider = fauxProvider({ provider: "openai", models: [{ id: "gpt-5-mini" }], tokensPerSecond: 50, tokenSize: { min: 1, max: 1 } })
mock.module("@earendil-works/pi-ai/providers/openai", () => ({ openaiProvider: () => provider.provider }))
const { PiAgent } = await import("../src/actors.js")
type ChatEvent = import("../src/actors.js").ChatEvent

test("the actor streams over its socket and recovers the native Pi conversation", async () => {
    const database = new Database(":memory:")
    const db: ActorDatabase = { exec: <Row extends object>(sql: string, ...bindings: SqliteValue[]) => database.query(sql).all(...bindings) as Row[] }
    const events: ChatEvent[] = []
    const socket: ActorSocket<{}, ChatEvent> = {
        id: "chat", metadata: {}, tags: [], state: "open",
        send: event => { events.push(event) },
        close() {}, reject() {}, setTags() {}
    }
    provider.setResponses([fauxAssistantMessage("Hello Alice. Your conversation is saved."), transcript => {
        expect(transcript.messages.some(message => message.role === "user" && message.content === "My name is Alice")).toBe(true)
        return fauxAssistantMessage("Your name is Alice")
    }])
    try {
        for (const prompt of ["My name is Alice", "What is my name?"]) {
            const actor = constructWithActorDatabase(db, () => Reflect.construct(PiAgent, []) as typeof PiAgent.prototype)
            await runWithActorDatabase(actor, "immediate", () => actor.onMessage(socket, prompt))
        }
        expect(events.some(event => event.type === "text" && event.text.length > 0 && event.text.length < 20)).toBe(true)
        expect(events.at(-1)).toEqual({ type: "done", text: "Your name is Alice" })
    } finally { database.close() }
})

test("the actor sends native model failures to the socket", async () => {
    const database = new Database(":memory:")
    const db: ActorDatabase = { exec: <Row extends object>(sql: string, ...bindings: SqliteValue[]) => database.query(sql).all(...bindings) as Row[] }
    const events: ChatEvent[] = []
    const socket: ActorSocket<{}, ChatEvent> = {
        id: "chat", metadata: {}, tags: [], state: "open",
        send: event => { events.push(event) },
        close() {}, reject() {}, setTags() {}
    }
    provider.setResponses([fauxAssistantMessage("", { stopReason: "aborted", errorMessage: "Request cancelled" })])
    try {
        const actor = constructWithActorDatabase(db, () => Reflect.construct(PiAgent, []) as typeof PiAgent.prototype)
        await runWithActorDatabase(actor, "immediate", () => actor.onMessage(socket, "Hello"))
        expect(events.at(-1)).toEqual({ type: "error", message: "Pi did not answer: model_error: Request cancelled" })
    } finally { database.close() }
})
