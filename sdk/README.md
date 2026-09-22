# Durable Actors

Named actors with serial method calls and saved state. Requires Node.js 20.19+ or 22.12+ (matching Vite's runtime requirement). The CLI uses Node.js; actor execution requires Bun 1.4.2+.

## Local development

Install the CLI once with pnpm:

```sh
pnpm add --global durable-actors
```

Create a standalone actor project and start its server:

```sh
durable-actors init my-actors
cd my-actors
pnpm install
durable-actors dev
```

The project contains a persisted counter in `src/durable-objects.ts`, TypeScript configuration, and scripts to check and build your actors. Keep the actor server running.

In your separate application project's directory, install the SDK:

```sh
pnpm add durable-actors
```

Copy the settings printed under **Connect your application** into that application's `.env` file:

```dotenv
DURABLE_ACTORS_PROJECT_ID=local
DURABLE_ACTORS_CONTROL_PLANE_URL=http://127.0.0.1:7100
DURABLE_ACTORS_SECRET='<paste the secret printed by dev>'
```

Generate the client from your application directory:

```sh
durable-actors generate
```

The CLI loads `.env` automatically and fetches the contract from your actor server. Use the generated client in your backend:

```ts
import { actors } from "./generated/index.js"

const counter = actors.Counter.get("example")
console.log(await counter.increment())
```

Start your backend with that `.env` loaded, using your usual development command. Generated clients contain public types without importing actor source or its private dependencies.

Edit actors in the actor project's `src/durable-objects.ts`. Annotate every instance field with `@Persisted` or `@Ephemeral`, imported from `durable-actors`. Rerun `durable-actors generate` in your application when actor method signatures change. `dev` mints a fresh shared secret on restart unless one is configured, so update your application's `.env` and restart its backend too.

For complete application templates, use `durable-actors init <directory> --template chat`, `--template ai-chat`, or `--template documents`. See the [chat](https://github.com/TerseAI/durable-actors/tree/main/examples/chat), [AI chat](https://github.com/TerseAI/durable-actors/tree/main/examples/ai-chat), and [documents](https://github.com/TerseAI/durable-actors/tree/main/examples/documents) examples.

## Generate clients programmatically

Build tools can generate the same files in memory through the public compiler and codegen APIs:

```ts
import { generateTypeScript } from "durable-actors/codegen"
import { ActorCompiler } from "durable-actors/compiler"

const contract = new ActorCompiler().compileContract("src/durable-objects.ts")
const files = await generateTypeScript(contract)
// files is a ReadonlyMap<string, string> of relative filenames to TypeScript source.
```

The caller chooses where to write the files. Generation does not execute actor code. The CLI writes only generated TypeScript clients.

## Deploy and generate remote clients

For a hosted deployment, configure the [remote connection](#hosted-backends), then supply your published customer build image. The control plane compiles the code and public contract, publishes the snapshot, and registers the deployment in one request:

```sh
durable-actors deploy --image im-customer-build
```

Then generate clients in another repository using the same environment settings:

```sh
durable-actors generate
```

Deploy assigns a revision automatically, and generation uses the latest deployment. The server stores only its active contract. See the [CLI reference](../docs/reference/cli.md#generate-from-the-control-plane) for deployment and credentials.

The npm package installs the `durable-actors` CLI. On first use, `dev` downloads and caches the matching native runtime automatically. It watches TypeScript files across the actor project and publishes valid source changes to the local control plane; rerun `durable-actors generate` when the public contract changes. Start your frontend and application backend with their usual tooling. Restart the application backend after restarting the actor runtime to reload cached settings. State survives restarts in `.durable-actors/`.

`durable-actors dev --help` lists options. There is no CLI client runner; browser applications use native WebSockets as shown below.

## Test runners

Start a local server programmatically and pass its connection settings to your test runner:

```ts
import { startLocalActors } from "durable-actors/dev"

const runtime = await startLocalActors({ entrypoint: "src/actors.ts" })
try {
    await runTests(runtime.connection)
} finally {
    await runtime.stop()
}
```

The launcher downloads the matching runtime and waits for readiness. It defaults to a free loopback port and `.durable-actors/`; set `project`, `dataDir`, or `port` to override them. `stop()` preserves state and waits for shutdown. `connection` exposes the server settings; `closed` rejects on process failure.

## Hosted backends

Configure the [remote connection](https://github.com/TerseAI/durable-actors/blob/main/docs/reference/configuration.md) once for your CLI and backend. Local development uses the connection settings printed by `dev`.

Keep the shared secret on your backend, where you check user permissions. The SDK connects to the named actor and calls its methods. Mobile and browser apps use [WebSockets authorized by your backend](https://github.com/TerseAI/durable-actors/blob/main/docs/guides/self-hosting.md#browser-connections).

See [self-hosting](https://github.com/TerseAI/durable-actors/blob/main/docs/guides/self-hosting.md) for deployment and credentials. Runtime distributions bundle the Go provider.

## WebSocket API

The gateway keeps connections while actors hibernate. Actors use `onConnect`, `onMessage`, and `onDisconnect` to manage application messages. Browser connections receive explicit actor messages plus automatic snapshots and committed updates of public `@Persisted @Emittable` fields. Private, protected, and non-emittable fields are excluded.

For long-running async methods, import the experimental `Reentrant` decorator and apply `@Reentrant` to allow other calls and socket hooks to run while the method awaits. The method retains actor state access, but state can change across awaits. Undecorated calls block all new invocations until completion and persistence; already-running reentrant continuations may still resume during their awaits. Direct `this.method()` calls inherit the current invocation's mode.

Declaring any reentrant method disables application-error rollback for the whole actor class. Failed-call mutations remain in memory and may be saved by another successful call. Persistence still occurs on successful completion, so this does not make streamed progress immediately durable. See the [scheduling and persistence contract](https://github.com/TerseAI/durable-actors/blob/main/docs/reference/api.md#reentrant-methods-experimental) before opting in.

Inside actor hooks and backend SDK connections, send JSON values with `socket.send({ type: "chat", text: "Hello" })`. The backend SDK encodes and parses these values. Native browser sockets use `JSON.stringify` and `JSON.parse`.

`Actor<Metadata, Incoming, Outgoing = Incoming, Tag extends string = string>` types metadata, both message directions, and tags. Use `ActorSocketOf<ChatRoom>` and `ActorMessageOf<ChatRoom>` in hooks to reuse those types. Generated JSON schemas are checked at deployment and are not enforced with AJV during execution. Optional static Zod schemas validate metadata, incoming and outgoing messages, and tags at runtime; see [generics and wire validation](https://github.com/TerseAI/durable-actors/blob/main/docs/reference/api.md#generics-and-wire-validation).

| API                                  | Behavior                                             |
| ------------------------------------ | ---------------------------------------------------- |
| `Actor.get(id).connect(metadata)`    | Opens a connection with JSON-serializable metadata.  |
| `onMessage(socket, message)`         | Handles incoming messages on the actor.              |
| `onDisconnect(socket)`               | Handles a closed connection.                         |
| `this.broadcast(message)`            | Sends to connected clients.                          |
| `socket.send(message)`               | Sends to one client.                                 |
| `socket.setTags(...tags)`            | Tags a connection for filtered broadcasts.           |
| `socket.close()` / `socket.reject()` | Closes a connection / rejects it during `onConnect`. |

`await this.getConnections()` fetches connections on demand and caches the result for that invocation. Ordinary methods that do not enumerate connections skip the gateway lookup. From application code, `Actor.get(id).broadcast(message)` sends transient output without invoking the actor or saving state.

For a separate WebSocket gateway, see [gateway configuration](https://github.com/TerseAI/durable-actors/blob/main/docs/reference/configuration.md).

## Browser clients

Generate backend helpers from your actor entrypoint:

```sh
npx durable-actors generate
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

The helper reads connection settings from environment variables. For overrides or an injected transport, use `actors.ChatRoom.prepareWebsocket(authorization, options, { fetch })`. `ActorProxy.handle({ actorName, actorId, metadata })` remains available for dynamic actor selection.

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

Messages are application JSON in text frames, with no SDK envelopes, initialization frames, or special subprotocol. Actors send initial application data explicitly from `onConnect` and use `socket.send()` or `this.broadcast()` for updates. Public `@Persisted @Emittable` fields also send automatic `state` snapshots and committed `state_update` messages. Actors without emittable fields send only application messages. Reconnection, message replay, and ticket renewal remain application responsibilities.

Open a grant within 60 seconds. Connection authorization defaults to 15 minutes; set `authorizationLifetimeMs` in `prepareWebsocket` to change it, subject to the server's maximum. The server closes expired connections with code `4408`, including while idle or running a handler. Your application decides whether to request a fresh grant and open another socket.

Treat both the URL and key as credentials. Keep the backend shared secret on the server and omit signed URL query strings from logs. Call `socket.close()` when the view is finished.

## Reference

- [Configuration](https://github.com/TerseAI/durable-actors/blob/main/docs/reference/configuration.md): local defaults, environment variables, credentials, and server settings.
- [CLI reference](https://github.com/TerseAI/durable-actors/blob/main/docs/reference/cli.md): commands and options.
- [TypeScript API reference](https://github.com/TerseAI/durable-actors/blob/main/docs/reference/api.md): actors, methods, connections, types, and errors.
- [HTTP and WebSocket reference](https://github.com/TerseAI/durable-actors/blob/main/docs/reference/http.md): deployments, backend access, connections, and callbacks.
- [Local development](https://github.com/TerseAI/durable-actors/blob/main/docs/guides/local-development.md): install from npm and run actors locally.

## License

MIT © 2026 Terse
