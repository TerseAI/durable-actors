# AI chat

An Express + React app that streams replies with the Vercel AI SDK and saves conversations in durable actors.

## Run locally

Requires Node.js 22.19+, Bun 1.4.2+, and an OpenAI API key.

```sh
npx durable-actors init ai-chat-example --template ai-chat
cd ai-chat-example
npm install
cp .env.example .env
```

Already in the example directory? Start at `npm install`. Add `OPENAI_API_KEY` to `.env`, then start the actors:

```sh
npm run dev:actors
```

Wait for `Ready`. In another terminal in the same directory:

```sh
npm run dev
```

Open [localhost:3000](http://127.0.0.1:3000), send a message, and reload after the reply finishes. The conversation survives server restarts.

## Save the conversation

[ChatHistory](src/actors.ts) keeps one conversation per actor ID:

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

## Stream the reply

The [Express backend](src/backend.ts) appends the user message, sends the saved conversation to the model, and streams the reply. It saves the assistant message when the reply completes.

The [React client](src/Chat.tsx) loads saved messages and uses `useChat` to display the stream. This demo saves text only; in-progress streams are not resumed after a reload.

`ChatMessage` is the JSON-compatible storage type; the backend converts AI SDK messages to it before appending. For arbitrary JSON metadata or tool data, import `JsonValue` or `JsonObject` from `durable-actors` instead of using the AI SDK's `unknown` fields in actor methods.

The lobby is shared and has no authentication. Add authentication and chat ownership checks before using it for private conversations.

## Development

Both processes read `.env`; actor state lives in `.durable-actors/`. Actor code reloads automatically; restart `npm run dev` after editing the actor class imported by the backend. For multiple examples, set distinct `PORT`, `DURABLE_ACTORS_PORT`, and matching control-plane URLs; see [Run the examples together](../README.md).

`npm run build` checks TypeScript and builds the frontend.
