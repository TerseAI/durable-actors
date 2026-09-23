# Durable Actors

Managing state is hard! Back in the pre-agent era, building a multiplayer app showed just how hard this could be. You had to lock resources, deal with websockets at scale, handle peak loads etc...

Now with AI, we've got agents working with agents and agents working with people to worry about. Furthermore, we have agent swarms coming!

Durable Actors is a primitive to help developers build the next generation of collaborative software. We provide a mechanism to serve shared, concurrency safe state to your app.

Based on the Actor principle from Erlang, all state is durably persisted for you. Only one Agent/person can be in the actor at a time, protecting you from race conditions.

We are fully horizontally scalable, and instances go dormant when not in use. Only pay for what your users are using.

We offer a clean API to manage webSocket connections, Swift inspired syntax for building your actor and full observability into your deployed actors.

## Local development

Install Node.js 22.19+, pnpm, and Bun 1.4.2+. Install the CLI once:

```sh
pnpm add --global durable-actors
```

### Create your actor project in your directory of choice

```sh
durable-actors init my-actors
cd my-actors
pnpm install
durable-actors dev // this will run the server locally on your machine
```

Running dev will also start a watch, every-time you make a change to an actor and save, metadata changes will be stored automatically.

### Connect your application

Your application does not need to install `durable-actors`. The generated client includes its runtime and types.

Copy the three settings printed by `dev` into your application's `.env` file:

```dotenv
DURABLE_ACTORS_PROJECT_ID='<paste the project ID printed by dev>'
DURABLE_ACTORS_CONTROL_PLANE_URL=http://127.0.0.1:7100
DURABLE_ACTORS_SECRET='<paste the secret printed by dev>'
```

Then use the CLI to generate a client from your running actor server:

```sh
durable-actors generate --remote
```

Keep the entire `generated/` directory, including `runtime/`, with your application. When compiling TypeScript with `tsc`, copy `generated/runtime/` alongside the emitted `index.js`; bundlers include the runtime automatically. Regenerate to pick up actor contract changes and SDK runtime updates.

Now you may call your actor and access the state.

```ts
import { actors } from "./generated/index.js"

const counter = actors.Counter.get("example")
console.log(await counter.increment())
```

For complete sample applications, see [AI Chat](examples/ai-chat), [Collaborative documents](examples/documents), and [Chatroom](examples/chat).

## Define an Actor

Define and export actors in your actor project’s `src/actors.ts`. For example, a chat history actor:

```ts
import { Actor, Persisted } from "durable-actors"

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

This example saves text parts only. Actor method arguments and results must have JSON-compatible types. For arbitrary JSON metadata or tool data, import `JsonValue` or `JsonObject` from `durable-actors`; the AI SDK's default `UIMessage` includes `unknown` fields that cannot be used directly in an actor contract.

## Stream from the backend (Express)

After adding `ChatHistory`, rerun `durable-actors generate` in your application and use its generated client:

```ts
import { openai } from "@ai-sdk/openai"
import { convertToModelMessages, generateId, pipeUIMessageStreamToResponse, streamText, toUIMessageStream, validateUIMessages } from "ai"
import type { UIMessage } from "ai"
import express from "express"

import { actors } from "./generated/index.js"

const app = express()
app.use(express.json())

app.get("/api/chat/:id", async (request, response) => {
    response.json(await actors.ChatHistory.get(request.params.id).load())
})

app.post("/api/chat", async (request, response) => {
    const [message] = await validateUIMessages({ messages: [request.body.messages.at(-1)] })
    if (message.role !== "user") return response.sendStatus(400)
    const chat = actors.ChatHistory.get(request.body.id)
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

function historyMessage(message: UIMessage) {
    return {
        id: message.id,
        role: message.role,
        parts: message.parts.filter(part => part.type === "text").map(part => ({ type: part.type, text: part.text }))
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

## License

MIT © 2026 Terse
