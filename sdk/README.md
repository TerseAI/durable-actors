# little-actors

Named actors with saved state. Requires Node.js ^20.19.0 or >=22.12.0 and Bun 1.4.2+.

```sh
npm install little-actors
```

For a complete app, run `npx little-actors init my-app` and follow its README. See [CLI workflows](https://github.com/TerseAI/little-actors/blob/main/docs/reference/cli.md) for deployment and other commands.

## Define an actor

Export actors from `src/durable-objects.ts`:

```ts
import { Actor, Persisted } from "little-actors"

export class Counter extends Actor {
    @Persisted count = 0

    async increment(): Promise<number> {
        return ++this.count
    }
}
```

Methods must be async. Mark each field `@Persisted` to save it or `@Ephemeral` for temporary values. Calls run sequentially by default. See the [TypeScript API](https://github.com/TerseAI/little-actors/blob/main/docs/reference/api.md) for behavior and the generated reference.

## Run locally

```sh
DURABLE_OBJECT_PROJECT_ID=my-project npx little-actors start --dev
```

Wait for `Ready`. In your backend terminal, set `DURABLE_OBJECT_PROJECT_ID=my-project` and run the printed `export DURABLE_OBJECT_API_KEY=…` command. Keep the actor server running; code reloads automatically and state survives restarts.

Call an actor from your backend:

```ts
import { Counter } from "./src/durable-objects.js"

const count = await Counter.get("visits").increment()
```

For a separate backend project, generate typed helpers with `npx little-actors generate` and import `actors` from `generated/index.js`.

See [configuration](https://github.com/TerseAI/little-actors/blob/main/docs/reference/configuration.md) for remote servers and credentials.

## Browser clients

Generate backend helpers with `npx little-actors generate`. Authenticate the user and check actor access before issuing a WebSocket URL. For a `ChatRoom` actor:

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

The browser connects using native WebSockets:

```js
const response = await fetch("/api/socket/ChatRoom/lobby", { method: "POST" })
if (!response.ok) throw new Error("Connection denied")
const { websocketUrl } = await response.json()
const socket = new WebSocket(websocketUrl)

socket.onopen = () => socket.send(JSON.stringify({ type: "post", text: "Hello" }))
socket.onmessage = event => renderMessage(JSON.parse(event.data))
```

Keep the API key on your backend and treat the returned URL as a credential. See [WebSockets](https://github.com/TerseAI/little-actors/blob/main/docs/guides/websockets.md) for state updates, expiration, and reconnecting.

## Test runners

```ts
import { startLocalActors } from "little-actors/dev"

const runtime = await startLocalActors({ projectId: "my-project", entrypoint: "src/durable-objects.ts" })
try {
    await runTests(runtime.connection)
} finally {
    await runtime.stop()
}
```

`stop()` preserves saved state. Set `dataDir` to isolate test runs.

## License

MIT © 2026 Terse
