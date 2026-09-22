# AI chat with durable history

Requires Node.js 20+ and Bun 1.4.2+ on your PATH; Bun executes the actors.

Vercel AI SDK streams replies; a durable actor stores the conversation.

## Run it

```sh
npx durable-actors init ai-chat-example --template ai-chat
cd ai-chat-example
npm install
```

Copy `.env.example` to `.env` and add your `OPENAI_API_KEY`, then start the actors:

```sh
npx durable-actors dev
```

Wait for `Local actors ready`. In another terminal, run the printed export command, then:

```sh
npm run dev
```

Open [the chat](http://127.0.0.1:3000), send a message, and reload after the reply finishes. Your history is restored from the actor, including after restarting the servers.

If you already have this directory, start at `npm install`. No client generation is needed for this example.

## The code

- [src/durable-objects.ts](src/durable-objects.ts) stores AI SDK messages in a private `@Persisted` field. Each chat ID gets its own actor.
- [src/backend.ts](src/backend.ts) loads saved history, appends the new user message, streams a reply, and saves the completed assistant message.
- [src/Chat.tsx](src/Chat.tsx) loads the lobby history and uses `useChat` to send messages and render streaming replies.

For a remote actor server, use the [connection overrides](../../docs/reference/configuration.md) on the backend.

This sample has one shared lobby and no authentication. Authenticate both routes and check chat ownership before using it for private conversations. Reloads restore saved messages; in-progress streams are not resumed.

See Vercel’s [message persistence guide](https://ai-sdk.dev/docs/ai-sdk-ui/chatbot-message-persistence) for the AI SDK flow.
