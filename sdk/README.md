# Durable Actors

Managing state is hard! Back in the pre-agent era, building a multiplayer app showed just how hard this could be. You had to lock resources, deal with websockets at scale, handle peak loads etc...

Now with AI, we've got agents working with agents and agents working with people to worry about. Furthermore, we have agent swarms coming!

Durable Actors is a primitive to help developers build the next generation of collaborative software. We provide a mechanism to serve shared, concurrency safe state to your app.

Based on the Actor principle from Erlang, all state is durably persisted for you. Only one Agent/person can be in the actor at a time, protecting you from race conditions.

We are fully horizontally scalable, and instances go dormant when not in use. Only pay for what your users are using.

We offer a clean API to manage webSocket connections, Swift inspired syntax for building your actor and full observability into your deployed actors.

## Local development

Install Node.js 22.19+, pnpm, and Bun 1.3.9+.

### Create your actor project in your directory of choice

```sh
npx durable-actors init my-actors
cd my-actors
pnpm install
pnpm exec durable-actors dev # Run the server locally on your machine
```

Running dev will also start a watch, every-time you make a change to an actor and save, metadata changes will be stored automatically.

### Connect your application

In your separate application project's directory (ex: node server), install the generator as a development dependency:

```sh
pnpm add -D durable-actors
pnpm add zod@4
```

Local CLI commands and backend clients default to project `local` at `http://127.0.0.1:7100`. No project ID or secret is required, and local authentication is disabled unless you set `DURABLE_ACTORS_SECRET` on the actor server and backend.

Then generate your client from the same application directory:

```sh
pnpm exec durable-actors generate
```

The generated directory contains the typed client and its runtime. Commit the whole directory; its only runtime dependency is Zod 4. Applications that only call actors do not need `durable-actors` in production. You can also run `pnpm dlx durable-actors generate` without adding the SDK to the application’s dependencies or lockfile. Pin the CLI version when using this in CI.

Regenerate after changing actor contracts or upgrading the generator to pick up runtime fixes.

Now you may call your actor and access the state.

```ts
import { actors } from "./generated/index.js"

const counter = actors.Counter.get("example")
console.log(await counter.increment())
```

For complete sample applications, see [AI Chat](https://github.com/TerseAI/durable-actors/tree/main/examples/ai-chat), [Collaborative documents](https://github.com/TerseAI/durable-actors/tree/main/examples/documents), and [Chatroom](https://github.com/TerseAI/durable-actors/tree/main/examples/chat).

## Define an Actor

Define and export actors in your actor project’s `src/durable-objects.ts`. For example, a chat history actor:

```ts
import type { UIMessage } from "ai"
import { Actor, Persisted } from "durable-actors"

export class ChatHistory extends Actor {
    @Persisted private messages: UIMessage[] = []

    async load() {
        return this.messages
    }

    async append(message: UIMessage) {
        this.messages.push(message)
        return this.messages
    }
}
```

## Stream from the backend (Express)

After adding `ChatHistory`, rerun `pnpm exec durable-actors generate` in your application and use its generated client:

```ts
import { openai } from "@ai-sdk/openai"
import { convertToModelMessages, generateId, pipeUIMessageStreamToResponse, streamText, toUIMessageStream, validateUIMessages } from "ai"
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
    const messages = await chat.append(message)
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
                if (outcome.status === "completed") await chat.append(responseMessage)
            }
        })
    })
})
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

## Host it yourself

Follow the [self-hosting guide](https://github.com/TerseAI/durable-actors/blob/main/docs/reference/self-hosting.md) to connect your backend with an API key and deploy your actors.

## License

MIT © 2026 Terse
