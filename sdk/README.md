# little-actors

Named actors with serial method calls and saved state. Requires Node.js 20+.

```sh
npm install little-actors
```

Start with the [chat example](https://github.com/TerseAI/little-actors/tree/main/examples/chat) to create and run the Express + React chat app.

## Local CLI

Create the bundled chat app in a new directory:

```sh
npx little-actors init chat-example
```

The command copies the template and prints setup instructions. Its dependencies include the same SDK version as the CLI. In an existing application, install `little-actors` and follow the actor setup below.

For Vercel AI SDK with durable chat history, use `npx little-actors init ai-chat-example --template ai-chat`. The [AI chat example](https://github.com/TerseAI/little-actors/tree/main/examples/ai-chat) uses `useChat`, HTTP streaming, and backend actor calls; it needs an OpenAI API key and no generated clients.

For a collaborative Tiptap editor, use `npx little-actors init documents-example --template documents`. The [documents example](https://github.com/TerseAI/little-actors/tree/main/examples/documents) uses Yjs, native WebSockets, and one durable actor per document.

Export actors from `src/durable-objects.ts`. Annotate every instance field with `@Persisted` or `@Ephemeral`, imported from `little-actors`. Persisted values survive restarts; ephemeral caches last only while the actor instance remains resident. In your project directory:

```sh
npx little-actors dev
```

Wait for `Local actors ready at http://127.0.0.1:7100`. The runtime generates an API key and saves it with the control plane URL in `.little-actors/runtime.json`. Backend actor calls and generated proxies discover both automatically from the working directory. Start your application backend from the same project directory; no environment variables are needed.

Generate source once for your backend and web app:

```sh
npx little-actors generate
```

Build tools can generate the same files in memory through the public compiler and codegen APIs:

```ts
import { generateTypeScript } from "little-actors/codegen"
import { ActorCompiler } from "little-actors/compiler"

const contract = new ActorCompiler().compileContract("src/durable-objects.ts")
const files = await generateTypeScript(contract)
// files is a ReadonlyMap<string, string> of relative filenames to TypeScript source.
```

The caller chooses where to write the files. Generation does not execute actor code. The CLI writes only generated TypeScript clients.

Generated `index.ts` exposes typed RPC stubs that work in a separate backend repository:

```ts
import { actors } from "./generated/index.js"

const room = actors.ChatRoom.get("lobby")
await room.sendMessage({ text: "Hello" }) // Arguments and return types come from your actor API.
```

The methods above assume your actor defines `sendMessage(input: { text: string })`. The generated stub uses the SDK's normal backend connection settings. Generated clients contain public types without importing the actor implementation or its private dependencies, so you can publish them as a separate npm package.

For a hosted deployment, configure the [remote connection](#hosted-backends) once, then register your built actor image from its source project. The CLI extracts and publishes the public contract automatically:

```sh
npx little-actors deploy --image im-chat --working-directory /app
```

Then generate clients in another repository using the same environment settings:

```sh
npx little-actors generate --url
```

Deploy assigns a revision automatically, and generation uses the latest deployment. The server stores only its active contract. See the [CLI reference](../docs/reference/cli.md#generate-from-the-control-plane) for deployment and credentials.

The npm package installs the `little-actors` CLI. On first use, `dev` downloads and caches the matching native runtime automatically. It watches TypeScript files across the actor project and publishes valid source changes to the local control plane; rerun `generate --url` when the public contract changes. Start your frontend and application backend with their usual tooling. Restart the application backend after restarting the actor runtime to reload cached settings. State survives restarts in `.little-actors/`.

`little-actors dev --help` lists options. There is no CLI client runner; browser applications use native WebSockets as shown below.

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

Configure the [remote connection](https://github.com/TerseAI/little-actors/blob/main/docs/reference/configuration.md) once for your CLI and backend. Local development needs no connection configuration.

Keep the API key on your backend, where you check user permissions. The SDK connects to the named actor and calls its methods. Mobile and browser apps use [WebSockets authorized by your backend](https://github.com/TerseAI/little-actors/blob/main/docs/guides/self-hosting.md#browser-connections).

See [self-hosting](https://github.com/TerseAI/little-actors/blob/main/docs/guides/self-hosting.md) for deployment and credentials. Runtime distributions bundle the Go provider.

## WebSocket API

The gateway keeps connections while actors hibernate. Actors use `onConnect`, `onMessage`, and `onDisconnect` to manage application messages. Browser connections receive only explicit actor messages. Backend connections opened with `Actor.get(id).connect()` also receive automatic public persisted state; private and protected fields are excluded.

Inside actor hooks and backend SDK connections, send JSON values with `socket.send({ type: "chat", text: "Hello" })`. The backend SDK encodes and parses these values. Native browser sockets use `JSON.stringify` and `JSON.parse`.

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

For a separate WebSocket gateway, see [gateway configuration](https://github.com/TerseAI/little-actors/blob/main/docs/reference/configuration.md).

## Browser clients

Generate backend helpers from your actor entrypoint:

```sh
npx little-actors generate
```

The generated `index.ts` exposes typed backend RPC stubs and WebSocket grants under `actors`. Your frontend uses the browser's native `WebSocket`; it needs no generated client or SDK import.

Your backend authenticates the user and checks actor access before issuing a grant:

```ts
import { actors } from "./generated/index.js"

export async function POST(request: Request) {
    const user = await requireUser(request)
    const roomId = "lobby"
    await requireRoomAccess(user, roomId)
    const grant = await actors.ChatRoom.prepareWebsocket({
        actorId: roomId,
        metadata: { userId: user.id }
    })
    return Response.json(grant, { headers: { "cache-control": "no-store" } })
}
```

`prepareWebsocket` returns `{ websocketUrl, key }`. The URL already includes the signed key and can be passed directly to `new WebSocket()`. The key grants socket access to one actor; it cannot invoke backend RPC methods or issue other keys. Metadata comes from your backend and is validated by the actor host.

The helper discovers local settings automatically. For overrides or an injected transport, use `actors.ChatRoom.prepareWebsocket(authorization, options, { fetch })`. `ActorProxy.handle({ actorType, actorId, metadata })` remains available for dynamic actor selection.

The frontend fetches your application endpoint and opens the returned URL:

```js
const response = await fetch("/api/socket/ChatRoom/lobby", { method: "POST" })
if (!response.ok) throw new Error("Connection denied")
const { websocketUrl } = await response.json()
const socket = new WebSocket(websocketUrl)

socket.onopen = () => socket.send(JSON.stringify({ type: "post", text: "Hello" }))
socket.onmessage = event => renderMessage(JSON.parse(event.data))
socket.onclose = event => showDisconnected(event.code)
socket.onerror = () => showConnectionError()
```

Messages are application JSON in text frames, with no SDK envelopes, initialization frames, or special subprotocol. Actors send initial application data explicitly from `onConnect` and use `socket.send()` or `this.broadcast()` for updates. Browser connections have no automatic state snapshots, subscriptions, reconnection, replay, or renewal.

Open a grant within 60 seconds. Connection authorization defaults to 15 minutes; set `authorizationLifetimeMs` in `prepareWebsocket` to change it, subject to the server's maximum. The server closes expired connections with code `4408`, including while idle or running a handler. Your application decides whether to request a fresh grant and open another socket.

Treat both the URL and key as credentials. Keep the backend API key on the server and omit signed URL query strings from logs. Call `socket.close()` when the view is finished.

## Reference

- [Configuration](https://github.com/TerseAI/little-actors/blob/main/docs/reference/configuration.md): local defaults, environment variables, credentials, and server settings.
- [CLI reference](https://github.com/TerseAI/little-actors/blob/main/docs/reference/cli.md): commands and options.
- [TypeScript API reference](https://github.com/TerseAI/little-actors/blob/main/docs/reference/api.md): actors, methods, connections, types, and errors.
- [HTTP and WebSocket reference](https://github.com/TerseAI/little-actors/blob/main/docs/reference/http.md): deployments, backend access, connections, and callbacks.
- [Local development](https://github.com/TerseAI/little-actors/blob/main/docs/guides/local-development.md): install from npm and run actors locally.

## License

MIT © 2026 Terse
