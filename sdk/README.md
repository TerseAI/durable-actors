# little-actors

Named actors with serial method calls and saved state. Requires Node.js 20+.

```sh
npm install little-actors
```

Start with the [quickstart](https://github.com/TerseAI/little-actors#quickstart) to create and run the Express + React chat app.

## Local CLI

Create the bundled chat app in a new directory:

```sh
npx little-actors init chat-example
```

The command copies the template and prints setup instructions. Its dependencies include the same SDK version as the CLI. In an existing application, install `little-actors` and follow the actor setup below.

For Vercel AI SDK with durable chat history, use `npx little-actors init ai-chat-example --template ai-chat`. The [AI chat example](https://github.com/TerseAI/little-actors/tree/main/examples/ai-chat) uses `useChat`, HTTP streaming, and backend actor calls; it needs an OpenAI API key and no generated clients.

For a collaborative Tiptap editor, use `npx little-actors init documents-example --template documents`. The [documents example](https://github.com/TerseAI/little-actors/tree/main/examples/documents) uses Yjs, generated WebSocket clients, and one durable actor per document.

Export actors from `src/durable-objects.ts`. Annotate every instance field with `@Persisted` or `@Ephemeral`, imported from `little-actors`. Persisted values survive restarts; ephemeral caches last only while the actor instance remains resident. In your project directory:

```sh
npx little-actors dev
```

Wait for `Local actors ready at http://127.0.0.1:7100`. Generate source once for your backend and web app:

```sh
npx little-actors generate
```

Build tools can generate the same files in memory through the public compiler and codegen APIs:

```ts
import { ActorCompiler } from "little-actors/compiler"
import { generateTypeScript } from "little-actors/codegen"

const actors = new ActorCompiler().compile("src/durable-objects.ts")
const files = await generateTypeScript(actors.map(actor => actor.contract))
// files is a ReadonlyMap<string, string> of relative filenames to TypeScript source.
```

The caller chooses where to write the files. Generation does not execute actor code.

The npm package installs the `little-actors` CLI. On first use, `dev` downloads and caches the matching native runtime automatically. `ActorProxy` reads `.little-actors/runtime.json` automatically, including fresh credentials after a restart. Start your frontend and application backend with their usual tooling. State survives restarts in `.little-actors/`.

`little-actors dev --help` lists options. There is no CLI client runner; browser applications use the generated WebSocket SDK below.

## Test runners

Start a local server programmatically and pass its connection settings to your test runner:

```ts
import { startLocalActors } from "little-actors/dev"

const runtime = await startLocalActors({ entrypoint: "src/actors.ts" })
try {
    await runTests(runtime.connection)
} finally {
    await runtime.stop()
}
```

The launcher downloads the matching runtime and waits for readiness. It defaults to a free loopback port and `.little-actors/`; set `project`, `dataDir`, or `port` to override them. `stop()` preserves state and waits for shutdown. `connection` exposes the server settings; `closed` rejects on process failure.

## Hosted backends

Set these before the first actor call:

```sh
export DURABLE_OBJECT_API_KEY='<your-api-key>'
export DURABLE_OBJECT_CONTROL_PLANE_URL='https://objects.example.com'
```

Keep the API key on your backend, where you check user permissions. The SDK connects to the named actor and calls its methods. Mobile and browser apps use [WebSockets authorized by your backend](https://github.com/TerseAI/little-actors/blob/main/docs/guides/self-hosting.md#websocket-configuration).

See [self-hosting](https://github.com/TerseAI/little-actors/blob/main/docs/guides/self-hosting.md) for deployment and credentials. Runtime distributions bundle the Go provider.

## WebSocket API

The gateway keeps connections while actors hibernate. Each accepted connection receives the public persisted fields automatically. Private and protected fields stay in durable storage and are excluded from socket state.

Send JSON values directly with `socket.send({ type: "chat", text: "Hello" })`. The SDK encodes outgoing messages and parses incoming messages, including the initial state.

`Actor<Metadata, Incoming, Outgoing = Incoming, Tag extends string = string>` types metadata, both message directions, and tags. Use `ActorSocketOf<ChatRoom>` and `ActorMessageOf<ChatRoom>` in hooks to reuse those types. Optional static Zod schemas validate metadata, incoming and outgoing messages, and tags at runtime; see [generics and wire validation](https://github.com/TerseAI/little-actors/blob/main/docs/reference/api.md#generics-and-wire-validation).

| API                                  | Behavior                                             |
| ------------------------------------ | ---------------------------------------------------- |
| `Actor.get(id).connect(metadata)`    | Opens a connection with JSON-serializable metadata.  |
| `onMessage(socket, message)`         | Handles incoming messages on the actor.              |
| `onDisconnect(socket)`               | Handles a closed connection.                         |
| `this.broadcast(message)`            | Sends to connected clients.                          |
| `socket.send(message)`               | Sends to one client.                                 |
| `socket.setTags(...tags)`            | Tags a connection for filtered broadcasts.           |
| `socket.close()` / `socket.reject()` | Closes a connection / rejects it during `onConnect`. |

`this.connections` lists connections during an invocation. From application code, `Actor.get(id).broadcast(message)` sends transient output without invoking the actor or saving state.

WebSockets use the control-plane URL unless `DURABLE_OBJECT_SOCKET_GATEWAY_URL` is set.

## Browser clients

Generate a browser client and backend proxy from your actor entrypoint:

```sh
npx little-actors generate
```

This writes typed actor descriptors and client/proxy entrypoints. The frontend imports `ActorClient` from the generated `index.ts`, which uses `little-actors/browser`. The backend imports `ActorProxy` from the generated `proxy.ts`, which uses `little-actors/proxy`. Neither entrypoint imports the actor implementation, and the browser entrypoint excludes the proxy. Share this directory between your frontend and backend, or copy the generated files into separate projects. Regenerate when the actor contract changes. The actor host validates metadata, incoming and outgoing messages, and persisted public state against the contract; browser and proxy descriptors contain no actor-specific runtime validators. Invalid socket operations fail at the host rather than throwing synchronously from the browser’s `send()`. Compatible added fields are accepted at runtime.

Stack `@Emittable` with `@Persisted` to publish a field's final value after each successful operation commits:

```ts
import { Actor, type ActorMessageOf, Emittable, Persisted } from "little-actors"

export class ChatRoom extends Actor<{ userId: string }, { type: "post"; text: string }> {
    @Persisted @Emittable messages: string[] = []
    @Persisted private moderationNotes: string[] = []

    async onMessage(_socket: unknown, message: ActorMessageOf<ChatRoom>) {
        this.messages.push(message.text)
    }
}
```

`@Emittable` supplements persistence; it requires a public persisted field. Nested mutations are detected. Repeated assignments within one method produce one final update; unchanged values and failed methods produce none. Socket payloads, metadata, and public persisted state must use JSON-compatible types. Generation rejects unsupported types such as `any`, `Date`, functions, and `bigint`; optional properties are supported.

Your backend authenticates the user and checks access before calling the proxy helper:

```ts
import { ActorProxy } from "./generated/proxy.js"

export async function POST(request: Request) {
    const user = await requireUser(request) // Your application's authentication.
    const roomId = "lobby"
    await requireRoomAccess(user, roomId) // Runs on every connection and renewal.
    const grant = await ActorProxy.handle({
        actorType: "ChatRoom",
        actorId: roomId,
        metadata: { userId: user.id }
    })
    return Response.json(grant, { headers: { "cache-control": "no-store" } })
}
```

The generated proxy restricts `actorType` to your actors and types `metadata` for the selected actor. Invalid metadata fails before ticket issuance. `ActorProxy.handle(authorization)` returns `{ websocketUrl, key }`; it constructs the control-plane request internally and throws if authorization fails. Return the result as JSON with `Cache-Control: no-store`.

With no configuration, the proxy reads `.little-actors/runtime.json` from the working directory. Set `DURABLE_OBJECT_CONTROL_PLANE_URL`, `DURABLE_OBJECT_API_KEY`, and optional `DURABLE_OBJECT_NAMESPACE_ID` for a remote server. The URL defaults to `http://127.0.0.1:7100` when no local runtime URL is available. An explicit URL or API key skips local-file credentials. An optional second argument overrides these settings. For a configured instance or an injected transport, use `new ActorProxy(options, { fetch })`; its `handle(authorization)` method has the same actor-specific types.

The frontend uses the default application route:

```ts
import { ActorClient } from "./generated/index.js"

const client = ActorClient()
const room = client.ChatRoom.get("lobby")
const unsubscribe = room.subscribe("messages", messages => renderMessages(messages))
room.on("error", error => console.error(error.message))
await room.connect()
room.send({ type: "post", text: "Hello" })

// When this view is finished:
unsubscribe()
room.close()
```

`ActorClient()` posts to `/api/socket/{actorType}/{actorId}` on the current origin without a request body. Mount your application handler at that route; it authenticates the user and supplies the actor and metadata to `ActorProxy`. Override the route with `ActorClient({ endpoint })`, where `endpoint` is a URL or a function of `{ actorType, actorId }`.

Use `room.on("message", handler)` for explicit actor messages. `room.state` holds the latest snapshot; `subscribe` immediately supplies a cached field value to late listeners. Reconnect supplies a fresh snapshot. `room.on("status", handler)` observes `idle`, `connecting`, `open`, `reconnecting`, `closed`, and `error`.

The SDK obtains and renews credentials automatically through your endpoint, normally every 12 minutes of a 15-minute authorization. Set `authorizationLifetimeMs` in the proxy authorization to change that duration; the server's issuer maximum still applies. Unchanged authorization renews over the existing socket and preserves actor-modified metadata and tags. Changed authorized metadata reconnects through `onConnect`.

Network failures use bounded exponential backoff with jitter. HTTP 401/403, protocol errors, and explicit close stop retries. Live events are not replayed, and `send` throws immediately while disconnected; messages are never queued or resent. Use the optional `fetch` client option to integrate your application's request authentication.

## Reference

- [CLI reference](https://github.com/TerseAI/little-actors/blob/main/docs/reference/cli.md): commands, options, and environment variables.
- [TypeScript API reference](https://github.com/TerseAI/little-actors/blob/main/docs/reference/api.md): actors, methods, connections, types, and errors.
- [HTTP and WebSocket reference](https://github.com/TerseAI/little-actors/blob/main/docs/reference/http.md): deployments, backend access, connections, and callbacks.
- [Local development](https://github.com/TerseAI/little-actors/blob/main/docs/guides/local-development.md): install from npm and run actors locally.
- [Advanced access configuration](https://github.com/TerseAI/little-actors/blob/main/docs/guides/advanced-access.md).

## License

MIT © 2026 Terse
