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

Install Node.js 20+ for the CLI and Bun 1.4.2+ for actor execution. Each actor runs in its own Bun/Rust process pair locally and its own Modal sandbox when hosted.

```sh
npx little-actors start --dev --project-id my-project
```

Set the same `DURABLE_OBJECT_PROJECT_ID` in your application backend. If startup generates a key, run the printed `export DURABLE_OBJECT_API_KEY=…` command in your application backend terminal.

To test Modal hosts against a control plane on your laptop, use the repository's [`pnpm run start:cloud` command](docs/guides/local-development.md#test-modal-hosts-against-a-local-control-plane).

The Terse development setup uses the dedicated control-plane endpoint `https://terse-little-actors.ngrok.app`. Set these values in the repository root `.env`, alongside the Modal, GCS, database, and authentication settings:

```dotenv
NGROK_DOMAIN=terse-little-actors.ngrok.app
DURABLE_OBJECT_CONTROL_PLANE_URL=https://terse-little-actors.ngrok.app
DURABLE_OBJECT_CONTROL_PLANE_BIND=127.0.0.1:7200
```

```sh
pnpm run start:cloud
```

Keep this domain separate from the Terse backend's tunnel. Other ngrok accounts should reserve their own domain and substitute it above. The command waits for the tunnel, then starts the control plane; Ctrl+C stops both.


To keep the Terse backend's ngrok API on port 4040, give this tunnel its own local config. Create `.little-actors/ngrok.yml` (already gitignored):

```yaml
version: 3
agent:
  web_addr: 127.0.0.1:4041
```

Add `NGROK_CONFIG=.little-actors/ngrok.yml` to the root `.env`, along with `NGROK_AUTH_TOKEN` for authentication. This config replaces ngrok's default config for this process only. Restart `pnpm run start:cloud` to apply it; the public endpoint and control-plane port stay the same. Leave `NGROK_CONFIG` unset to use ngrok's default config.


## Define an Actor

```ts
import type { UIMessage } from "ai"
import { Actor, Persisted } from "little-actors"

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

```ts
import { openai } from "@ai-sdk/openai"
import { convertToModelMessages, generateId, pipeUIMessageStreamToResponse, streamText, toUIMessageStream, validateUIMessages } from "ai"
import express from "express"

import { ChatHistory } from "./durable-objects.js"

const app = express()
app.use(express.json())

app.get("/api/chat/:id", async (request, response) => {
    response.json(await ChatHistory.get(request.params.id).load())
})

app.post("/api/chat", async (request, response) => {
    const [message] = await validateUIMessages({ messages: [request.body.messages.at(-1)] })
    if (message.role !== "user") return response.sendStatus(400)
    const chat = ChatHistory.get(request.body.id)
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

Follow the [self-hosting guide](docs/guides/self-hosting.md) to connect your backend with an API key and deploy your actors.

## Reference

- [Configuration](docs/reference/configuration.md): local defaults, environment variables, credentials, and server settings.
- [CLI workflows](docs/reference/cli.md): running actors, generating clients, and opening the observability UI.
- [TypeScript API](docs/reference/api.md): actor classes, methods, connections, types, and errors.
- [HTTP API (OpenAPI)](docs/reference/openapi.yaml): deployments, backend access, WebSockets, and callbacks.

See the [documentation index](docs/README.md). The runtime serves its HTTP specification at `/openapi.yaml`. Run `pnpm docs:build` to generate the TypeScript reference in `.artifacts/api/`.

## License

MIT © 2026 Terse
