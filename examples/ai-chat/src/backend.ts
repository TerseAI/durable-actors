import { openai } from "@ai-sdk/openai"
import { type UIMessage, convertToModelMessages, generateId, pipeUIMessageStreamToResponse, streamText, toUIMessageStream, validateUIMessages } from "ai"
import express from "express"
import { createServer as createHttpServer } from "node:http"
import { createServer } from "vite"

import { ChatHistory, type StoredMessage } from "./actors.js"

const app = express()
const server = createHttpServer(app)
const port = Number(process.env.PORT ?? 3000)
app.use(express.json())

app.get("/api/chat/:id", async (request, response) => {
    response.json(await ChatHistory.get(request.params.id).load())
})

app.post("/api/chat", async (request, response) => {
    const [message] = await validateUIMessages({ messages: [request.body.messages.at(-1)] })
    if (message.role !== "user") return response.sendStatus(400)
    const chat = ChatHistory.get(request.body.id)
    const messages = await chat.append(toStoredMessage(message))
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
                if (outcome.status === "completed") await chat.append(toStoredMessage(responseMessage))
            }
        })
    })
})

const vite = await createServer({
    server: { middlewareMode: true, hmr: { server }, fs: { deny: [".env", ".env.*", "**/.durable-actors/**", "**/.git/**"] } }
})
app.use(vite.middlewares)
server.listen(port, "127.0.0.1", () => console.log(`AI chat: http://127.0.0.1:${port}`))

function toStoredMessage(message: UIMessage): StoredMessage {
    return {
        id: message.id,
        role: message.role,
        parts: message.parts.flatMap(part => (part.type === "text" ? [{ type: "text", text: part.text }] : []))
    }
}
