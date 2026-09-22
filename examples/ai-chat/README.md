# AI chat with durable history

Requires Node.js 22.19+ and Bun 1.4.2+ on your PATH; Bun executes the actors.

Vercel AI SDK streams replies; a durable actor stores the conversation.

## Run it

```sh
npx little-actors init ai-chat-example --template ai-chat
cd ai-chat-example
npm install
cp .env.example .env
```

Add your `OPENAI_API_KEY` to `.env`, then start the actors:

```sh
npm run dev:actors
```

Wait for `Ready`. In another terminal in this directory, start the application:

```sh
npm run dev
```

Open [the chat](http://127.0.0.1:3000), send a message, and reload after the reply finishes. Your history is restored from the actor, including after restarting the servers.

If you already have this directory, start at `npm install`. No client generation is needed for this example.

Both processes read the project ID, local development API key, and control-plane URL from `.env`. `dev:actors` runs the actors; `dev` starts Express and Vite. Run one example at a time with the default ports.

`npm run build` checks TypeScript and builds the frontend. The actor server watches source changes; restart the application backend after editing the actor class it imports.

## The code

- [src/durable-objects.ts](src/durable-objects.ts) stores message IDs, roles, and text parts in a private `@Persisted` field. Its concrete `ChatMessage` type is compatible with the actor compiler's JSON contract. Each chat ID gets its own actor.
- [src/backend.ts](src/backend.ts) loads saved history, appends the new user message, streams a reply, and saves the completed assistant text. This text-only demo omits tool calls, reasoning, and provider metadata from saved history.
- [src/Chat.tsx](src/Chat.tsx) loads the lobby history and uses `useChat` to send messages and render streaming replies.

For a remote actor server, use the [connection overrides](../../docs/reference/configuration.md) on the backend.

This sample has one shared lobby and no authentication. Authenticate both routes and check chat ownership before using it for private conversations. Reloads restore saved messages; in-progress streams are not resumed.

See Vercel’s [message persistence guide](https://ai-sdk.dev/docs/ai-sdk-ui/chatbot-message-persistence) for the AI SDK flow.
