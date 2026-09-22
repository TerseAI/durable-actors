# little-actors

little-actors is a framework for durable actors, powered by Rust. It's the easiest way to get started testing actors locally and can be extended to complex production deployments.

Durable Actors are TypeScript classes that persist their own state.

## Installation

```sh
npm install little-actors
```

See the sample apps:

- [AI Chat](examples/ai-chat)
- [Collaborative documents](examples/documents)
- [Chatroom](examples/chat)

## Run locally

Use Node.js 22.19+ and Bun 1.4.2+. Export your actors from `src/durable-objects.ts`, then start the actor server:

```sh
DURABLE_OBJECT_PROJECT_ID=my-project npx little-actors start --dev
```

Set the same `DURABLE_OBJECT_PROJECT_ID` in your application backend. If startup generates a key, run the printed `export DURABLE_OBJECT_API_KEY=…` command in your backend terminal. You can also keep these settings in `.env`; make sure your backend loads it.

Actor code reloads automatically, and saved state survives restarts.

For repository builds and local Modal testing, see [Contributing](CONTRIBUTING.md).

## Define an Actor

Export a `ChatHistory` actor to save each conversation:

```ts
import { Actor, Persisted } from "little-actors"

export class ChatHistory extends Actor {
    @Persisted private messages: ChatMessage[] = []

    async load() {
        return this.messages
    }

    async append(message: ChatMessage) {
        this.messages.push(message)
        return this.messages
    }
}

export type ChatMessage = {
    id: string
    role: "system" | "user" | "assistant"
    parts: { type: "text"; text: string }[]
}
```

## Stream from the backend (Express)

Use backend actor calls to load history and save completed replies:

```ts
import { openai } from "@ai-sdk/openai"
import { convertToModelMessages, generateId, pipeUIMessageStreamToResponse, streamText, toUIMessageStream, validateUIMessages } from "ai"
import type { UIMessage } from "ai"
import express from "express"

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

function historyMessage(message: UIMessage): ChatMessage {
    return {
        id: message.id,
        role: message.role,
        parts: message.parts.filter(part => part.type === "text").map(part => ({ type: "text", text: part.text }))
    }
}
```

## Connect the frontend (React)

```tsx
import { useChat } from "@ai-sdk/react"
import type { UIMessage } from "ai"

const history: UIMessage[] = await fetch("/api/chat/lobby").then(response => response.json())

function Chat() {
    const { messages, sendMessage, status } = useChat({ id: "lobby", messages: history })
    const busy = status === "submitted" || status === "streaming"

    return (
        <>
            {messages.map(message => (
                <p key={message.id}>
                    {message.role}: {message.parts.map(part => (part.type === "text" ? part.text : "")).join("")}
                </p>
            ))}
            <form
                action={async form => {
                    await sendMessage({ text: String(form.get("message")) })
                }}
            >
                <input name="message" aria-label="Message" required disabled={busy} />
                <button disabled={busy}>Send</button>
            </form>
        </>
    )
}
```

The [AI chat example](examples/ai-chat) includes the complete Express and React app. It saves text messages; reload after a reply finishes to restore the conversation. Add authentication and chat access checks before using it for private conversations.

## Host it yourself

A hosted server uses Modal, PostgreSQL, and GCS. See [server configuration](docs/reference/configuration.md#server-hosting) for the required settings.

With an existing server, set its URL, API key, and your project ID in `.env`, then register your published actor image:

```sh
npx little-actors deploy --image im-YOUR_IMAGE_ID
npx little-actors generate --remote
```

Each deployment replaces the current code and restarts actors while keeping saved state. Generated clients use the current deployed API.

## Reference

- [Configuration](docs/reference/configuration.md): environment variables and defaults.
- [HTTP API](docs/reference/openapi.yaml): OpenAPI, also served at `/openapi.yaml`.
- [TypeScript](docs/README.md): generated TypeDoc and editor hover documentation.
- CLI: `npx little-actors <command> --help`.

## License

MIT © 2026 Terse
