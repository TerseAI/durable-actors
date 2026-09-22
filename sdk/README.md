# durable-actors

Durable actors are TypeScript classes that persist their own state. Call them from your backend or connect browsers over WebSockets.

## Installation

Requires Node.js ^20.19.0 or >=22.12.0 and Bun 1.4.2+.

```sh
npm install durable-actors
```

For a complete app, try [chat](https://github.com/TerseAI/durable-actors/tree/main/examples/chat), [AI chat](https://github.com/TerseAI/durable-actors/tree/main/examples/ai-chat), or [collaborative documents](https://github.com/TerseAI/durable-actors/tree/main/examples/documents).

## Define an actor

Export actors from `src/durable-objects.ts`:

```ts
import { Actor, Persisted } from "durable-actors"

export class Counter extends Actor {
    @Persisted count = 0

    async increment(): Promise<number> {
        return ++this.count
    }
}
```

Methods must be async. Mark every instance field `@Persisted` to save it or `@Ephemeral` for temporary values. Use JSON values for state, arguments, and results.

## Run locally

Set your local connection in `.env`:

```dotenv
DURABLE_ACTORS_PROJECT_ID=my-project
DURABLE_ACTORS_SECRET=local-development-key
DURABLE_ACTORS_CONTROL_PLANE_URL=http://127.0.0.1:7100
```

```sh
npx durable-actors dev
```

Wait for `Ready`. Actor code reloads automatically, and state is saved in `.durable-actors/` across restarts.

## Call from your backend

Load the same environment in your backend, then call an actor by ID:

```ts
import { Counter } from "./durable-objects.js"

const count = await Counter.get("visits").increment()
```

Reusing the project ID, class name, and actor ID accesses the same saved state. Calls run sequentially by default. Await all work before returning from an actor method.

Failed calls roll back saved-state changes unless the actor uses `@Reentrant`. External effects cannot be undone. An `ActorInvocationError` with `code: "outcome_unknown"` means the operation may already have completed; retrying can run it twice.

## Connect a browser

Run `npx durable-actors generate` to create backend helpers. Your backend authenticates users, checks actor access, and calls `prepareWebsocket` to issue a connection URL. The browser passes that URL to `new WebSocket()`.

See the [chat backend](https://github.com/TerseAI/durable-actors/blob/main/examples/chat/src/backend.ts) and [React client](https://github.com/TerseAI/durable-actors/blob/main/examples/chat/src/Chat.tsx) for a working example. Keep the API key on your backend; your app handles reconnecting when a connection closes or expires.

## Deploy

Set your server URL, API key, and project ID in `.env`. With a published actor image:

```sh
npx durable-actors deploy --image im-YOUR_IMAGE_ID
npx durable-actors generate --remote
```

Deploy replaces the current code and restarts actors while keeping saved state. Import `actors` from `generated/index.js` in a separate backend project; regenerate when the deployed API changes.

## Reference

- [Configuration](https://github.com/TerseAI/durable-actors/blob/main/docs/reference/configuration.md): environment variables and defaults.
- [API references](https://github.com/TerseAI/durable-actors/blob/main/docs/README.md): TypeScript and HTTP.
- CLI: `npx durable-actors <command> --help`.

## License

MIT © 2026 Terse
