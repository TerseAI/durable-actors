import { createModels } from "@earendil-works/pi-ai/models"
import { fauxAssistantMessage, fauxProvider, fauxText, fauxThinking } from "@earendil-works/pi-ai/providers/faux"
import { MemoryStorage, createRegistry } from "@earendil-works/pi-durable"
import { expect, spyOn, test } from "bun:test"

import { PiSession } from "../src/session.js"

function setup() {
    const provider = fauxProvider({ tokensPerSecond: 0 })
    const models = createModels()
    models.setProvider(provider.provider)
    const model = provider.getModel()
    const storage = new MemoryStorage()
    const close = spyOn(storage, "close")
    const session = new PiSession(() => storage, { models, registry: createRegistry(), model: { provider: model.provider, modelId: model.id } })
    return { session, provider, close, storage }
}

test("a prompt returns the assistant's text and closes its session", async () => {
    const { session, provider, close } = setup()
    provider.setResponses([fauxAssistantMessage([fauxThinking("Private reasoning"), fauxText("Hello Alice"), fauxText("How can I help?")])])

    expect(await session.prompt("Hello")).toBe("Hello Alice\nHow can I help?")
    expect(close).toHaveBeenCalledTimes(1)
})

test("an unanswered prompt reports the reason and closes its session", async () => {
    const { session, provider, close } = setup()
    provider.setResponses([fauxAssistantMessage("", { stopReason: "aborted", errorMessage: "Request cancelled" })])

    await expect(session.prompt("Hello")).rejects.toThrow("Pi did not answer: model_error: Request cancelled")
    expect(close).toHaveBeenCalledTimes(1)
})

test("a database failure rejects the prompt and closes its session", async () => {
    const { session, storage, close } = setup()
    spyOn(storage, "commit").mockRejectedValue(new Error("disk failure"))

    await expect(session.prompt("Hello")).rejects.toThrow("disk failure")
    expect(close).toHaveBeenCalledTimes(1)
})

test("native session access returns the callback result and closes the harness", async () => {
    const { session, close } = setup()
    const result = await session.run(async ({ harness, conversation, context }) => {
        const root = await harness.root(context)
        return root.id === conversation.id ? "ready" : "wrong conversation"
    })
    expect(result).toBe("ready")
    expect(close).toHaveBeenCalledTimes(1)
})

test("native session access closes the harness when the callback fails", async () => {
    const { session, close } = setup()
    const failure = new Error("integration failure")
    await expect(
        session.run(() => {
            throw failure
        })
    ).rejects.toBe(failure)
    expect(close).toHaveBeenCalledTimes(1)
})
