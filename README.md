# Durable Actors

Durable Actors is a framework for durable actors, powered by Rust. It's the easiest way to get started testing actors locally and can be extended to complex production deployments.

Durable Actors are TypeScript classes that persist their own state.

## Local development

Install Node.js 22.19+, pnpm, and Bun 1.4.2+. Install the CLI once:

```sh
pnpm add --global durable-actors
```

### Create your actor project

```sh
durable-actors init my-actors
cd my-actors
pnpm install
durable-actors dev
```

`init` creates a standalone actor project with a persisted counter in `src/durable-objects.ts`. `dev` starts the actor server, watches your source, and downloads the matching native runtime on first use. Keep it running while you develop your application.

### Connect your application

In your separate application project's directory, install the SDK:

```sh
pnpm add durable-actors
```

Copy the three settings printed by `dev` into that application's `.env` file:

```dotenv
DURABLE_ACTORS_PROJECT_ID=local
DURABLE_ACTORS_CONTROL_PLANE_URL=http://127.0.0.1:7100
DURABLE_ACTORS_SECRET='<paste the secret printed by dev>'
```

Then generate your client from the same application directory:

```sh
durable-actors generate
```

The CLI loads `.env` automatically and fetches the actor contract from the running server. Your backend imports the generated client:

```ts
import { actors } from "./generated/index.js"

const counter = actors.Counter.get("example")
console.log(await counter.increment())
```

Start your application backend with its `.env` loaded, using your usual development command. Actor source stays in the actor project; your application uses the generated client.

Edit `src/durable-objects.ts` in the actor project while `dev` is running. Rerun `durable-actors generate` in your application when actor method signatures change. Restarting `dev` mints a new secret unless one is configured, so update your application's `.env` and restart its backend too.

For complete sample applications, see [AI Chat](examples/ai-chat), [Collaborative documents](examples/documents), and [Chatroom](examples/chat).

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

After adding `ChatHistory`, rerun `durable-actors generate` in your application and use its generated client:

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

Follow the [self-hosting guide](docs/guides/self-hosting.md) to connect your backend with an API key and deploy your actors.

See [bucket authority and replication](docs/guides/replication.md) for ownership, leases, storage layout, and replica placement.

## Test Modal hosts locally

To test Modal hosts against a control plane on your laptop, use the repository's [`pnpm run start:cloud` command](docs/guides/local-development.md#test-modal-hosts-against-a-local-control-plane).

Reserve a dedicated ngrok domain for your control plane. Substitute your domain in these values in the repository root `.env`, alongside the Modal, GCS, database, and authentication settings:

```dotenv
NGROK_DOMAIN=YOUR_DOMAIN.ngrok.app
DURABLE_OBJECT_CONTROL_PLANE_URL=https://YOUR_DOMAIN.ngrok.app
DURABLE_OBJECT_CONTROL_PLANE_BIND=127.0.0.1:7200
```

```sh
pnpm run start:cloud
```

Keep this domain separate from the Terse backend's tunnel. Other ngrok accounts should reserve their own domain and substitute it above. The command waits for the tunnel, then starts the control plane; Ctrl+C stops both.


To keep the Terse backend's ngrok API on port 4040, give this tunnel its own local config. Create `.durable-actors/ngrok.yml` (already gitignored):

```yaml
version: 3
agent:
  web_addr: 127.0.0.1:4041
```

Add `NGROK_CONFIG=.durable-actors/ngrok.yml` to the root `.env`, along with `NGROK_AUTH_TOKEN` for authentication. This config replaces ngrok's default config for this process only. Restart `pnpm run start:cloud` to apply it; the public endpoint and control-plane port stay the same. Leave `NGROK_CONFIG` unset to use ngrok's default config.

## Reference

- [Configuration](docs/reference/configuration.md): local defaults, environment variables, credentials, and server settings.
- [CLI reference](docs/reference/cli.md): running actors, generating SDKs, and command options.
- [TypeScript API reference](docs/reference/api.md): actor classes, methods, connections, types, and errors.
- [HTTP and WebSocket reference](docs/reference/http.md): deployments, backend access, WebSockets, and callbacks.

## License

MIT © 2026 Terse
