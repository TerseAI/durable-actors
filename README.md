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
# Or with npm:
npm install
npx durable-actors dev # Run the server locally on your machine
```

Running dev will also start a watch, every-time you make a change to an actor and save, metadata changes will be stored automatically.

### Connect your application

In your separate application project's directory (ex: node server), install the generator as a development dependency:

```sh
pnpm install --save-dev durable-actors
# Or with npm:
npm install --save-dev durable-actors
```

Then generate your client from the same application directory:

```sh
npx durable-actors generate
```

Now you may call your actor and access the state.

```ts
import { actors } from "./generated/index.js"

const counter = actors.Counter.get("example")
console.log(await counter.increment())
```

For complete sample applications, see [AI Chat](examples/ai-chat), [Collaborative documents](examples/documents), and [Chatroom](examples/chat).

## Streaming AI chat

One actor owns each conversation: it generates replies, streams them to every connected client over WebSockets, and persists the messages when the reply finishes. Messages are plain `{ role, content }` objects.

### Actor

Install `ai` and `@ai-sdk/openai` in your actor project, and set `OPENAI_API_KEY` in its `.env` file:

```sh
pnpm install ai @ai-sdk/openai
# Or with npm:
npm install ai @ai-sdk/openai
```

Define and export `ChatHistory` in your actor project's `src/actors.ts`:

```ts
import { openai } from "@ai-sdk/openai"
import { streamText } from "ai"
import { Actor, Persisted, type ActorSocket } from "durable-actors"

type Member = { name: string }
type Message = { role: "user" | "assistant"; content: string }
type Chat = { messages: Message[]; busy: boolean }

export class ChatHistory extends Actor<Member, string, Chat> {
    @Persisted messages: Message[] = []

    async onConnect(socket: ActorSocket<Member, Chat>) {
        socket.send({ messages: this.messages, busy: false })
    }

    async onMessage(socket: ActorSocket<Member, Chat>, text: string) {
        const messages: Message[] = [...this.messages, { role: "user", content: `${socket.metadata.name}: ${text}` }]
        this.broadcast({ messages, busy: true })

        const reply: Message = { role: "assistant", content: "" }
        const result = streamText({ model: openai("gpt-5-mini"), messages })
        for await (const chunk of result.textStream) {
            reply.content += chunk
            this.broadcast({ messages: [...messages, reply], busy: true })
        }

        this.messages = [...messages, reply]
        this.broadcast({ messages: this.messages, busy: false })
    }
}
```

The actor broadcasts the user's message immediately, streams the reply to all connected clients, and clears `busy` when it finishes. Replies run one at a time per conversation.

### Backend (Express)

In your application project, rerun `npx durable-actors generate` after adding the actor. Express only issues the WebSocket grant; the display name becomes connection metadata available to the actor.

`src/app.ts`:

```ts
import express from "express"

import { actors } from "../generated/index.js"

export const app = express()

app.post("/api/chat/:room/socket", async (req, res) => {
    const grant = await actors.ChatHistory.prepareWebsocket({
        actorId: req.params.room,
        metadata: { name: String(req.query.name ?? "Guest") }
    })
    res.set("Cache-Control", "no-store").json(grant)
})
```

Use this app in your HTTP server and serve the frontend from the same origin. Install `express`, `react`, and `react-dom` in the application project.

### Frontend (React)

In a React 19 browser app with a `root` element, each socket update replaces the displayed conversation. Sending stays disabled until the initial history arrives and while a reply is streaming.

`src/Chat.tsx`:

```tsx
import { useEffect, useRef, useState } from "react"
import { createRoot } from "react-dom/client"

import type { actors } from "../generated/index.js"

const params = new URLSearchParams(location.search)
const room = params.get("chat") ?? "lobby"
const name = params.get("name") ?? "Guest"

function Chat() {
    const socket = useRef<WebSocket>(null)
    const [chat, setChat] = useState<actors.ChatHistory.Outgoing>({ messages: [], busy: true })

    useEffect(() => {
        let active = true
        async function connect() {
            const response = await fetch(`/api/chat/${encodeURIComponent(room)}/socket?name=${encodeURIComponent(name)}`, { method: "POST" })
            const { websocketUrl } = await response.json()
            if (!active) return
            socket.current = new WebSocket(websocketUrl)
            socket.current.onmessage = event => setChat(JSON.parse(event.data))
        }
        void connect()
        return () => {
            active = false
            socket.current?.close()
        }
    }, [])

    function send(form: FormData) {
        const text = String(form.get("message")).trim()
        if (!text || socket.current?.readyState !== WebSocket.OPEN) return
        socket.current.send(JSON.stringify(text))
        setChat(chat => ({ ...chat, busy: true }))
    }

    return (
        <main>
            <h1>AI chat · {room}</h1>
            <div role="log" aria-label="Messages">
                {chat.messages.map((message, index) => (
                    <article key={index}>
                        <strong>{message.role}</strong>
                        <p>{message.content}</p>
                    </article>
                ))}
            </div>
            <form action={send}>
                <input name="message" aria-label="Message" required disabled={chat.busy} />
                <button disabled={chat.busy}>Send</button>
            </form>
        </main>
    )
}

createRoot(document.getElementById("root")!).render(<Chat />)
```

Open `/?name=Alice` and `/?name=Bob` in separate tabs to share the lobby. Add `&chat=another-room` for a separate conversation; unnamed visitors use `Guest`. Reload to restore saved history. New connections wait for an active reply to finish.

## License

MIT © 2026 Terse
