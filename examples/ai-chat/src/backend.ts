import { openai } from "@ai-sdk/openai"
import { convertToModelMessages, generateId, pipeUIMessageStreamToResponse, streamText, toUIMessageStream, validateUIMessages } from "ai"
import type { UIMessage } from "ai"
import express from "express"
import { createServer } from "vite"

import { ChatHistory } from "./durable-objects.js"
import type { ChatMessage } from "./durable-objects.js"

const app = express()
app.use(express.json())

app.get("/api/chat/:id", async (request, response) => {
    response.json(await ChatHistory.get(request.params.id).load())
})

app.post("/api/chat", async (request, response) => {
    const [message] = await validateUIMessages({ messages: [request.body.messages.at(-1)] })
    if (message.role !== "user") return response.sendStatus(400)
    const chat = ChatHistory.get(request.body.id)
    const messages = await chat.append(historyMessage(message))
    const result = streamText({
        model: openai("gpt-5-mini"),
        messages: await convertToModelMessages(messages)
    })
    await pipeUIMessageStreamToResponse({
        response,
        stream: toUIMessageStream({
            stream: result.stream,
            originalMessages: messages,
            generateMessageId: generateId,
            onEnd: async ({ responseMessage, outcome }) => {
                if (outcome.status === "completed") await chat.append(historyMessage(responseMessage))
            }
        })
    })
})

const vite = await createServer({
    server: { middlewareMode: true, fs: { deny: [".env", ".env.*", "**/.little-actors/**", "**/.git/**"] } }
})
app.use(vite.middlewares)
app.listen(3000, "127.0.0.1", () => console.log("AI chat: http://127.0.0.1:3000"))

function historyMessage(message: UIMessage): ChatMessage {
    return {
        id: message.id,
        role: message.role,
        parts: message.parts.filter(part => part.type === "text").map(part => ({ type: "text", text: part.text }))
    }
}
