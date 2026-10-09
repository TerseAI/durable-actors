import { createAssistantMessageEventStream, createModels, fauxAssistantMessage } from "@earendil-works/pi-ai"
import { openaiProvider } from "@earendil-works/pi-ai/providers/openai"
import { expect, test } from "bun:test"

import { PiChat } from "../src/chat.js"

test("Pi sends the prompt to the model and returns its text", async () => {
    const models = createModels()
    models.setProvider(openaiProvider())
    models.streamSimple = (_model, context) => {
        expect(context.messages.at(-1)?.content).toEqual([{ type: "text", text: "Hi Pi" }])
        const stream = createAssistantMessageEventStream()
        stream.push({ type: "done", reason: "stop", message: fauxAssistantMessage("Hello from Pi!") })
        return stream
    }
    expect((await new PiChat(models).prompt("Hi Pi", [])).text).toBe("Hello from Pi!")
})

test("Pi model errors are reported to the caller", async () => {
    const models = createModels()
    models.setProvider(openaiProvider())
    models.streamSimple = () => {
        const stream = createAssistantMessageEventStream()
        stream.push({
            type: "error",
            reason: "error",
            error: fauxAssistantMessage("", {
                stopReason: "error",
                errorMessage: "Provider unavailable"
            })
        })
        return stream
    }
    await expect(new PiChat(models).prompt("Hi Pi", [])).rejects.toThrow("Provider unavailable")
})

test("Pi continues from a saved conversation in a fresh agent", async () => {
    const models = createModels()
    models.setProvider(openaiProvider())
    let calls = 0
    models.streamSimple = (_model, context) => {
        if (calls++ === 1) {
            expect(context.messages.filter(message => message.role === "user")).toHaveLength(2)
            expect(context.messages.some(message => message.role === "assistant" && message.content.some(block => block.type === "text" && block.text === "Hello from Pi!"))).toBe(true)
        }
        const stream = createAssistantMessageEventStream()
        stream.push({ type: "done", reason: "stop", message: fauxAssistantMessage("Hello from Pi!") })
        return stream
    }
    const first = await new PiChat(models).prompt("My name is Thomas", [])
    const saved = JSON.parse(JSON.stringify(first.messages))
    const second = await new PiChat(models).prompt("What's my name?", saved)
    expect(second.messages.filter(message => message.role === "user")).toHaveLength(2)
    expect(saved).toEqual(first.messages)
})
