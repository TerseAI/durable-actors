# Actors

Requires Node.js 22.19.0+, pnpm, and Bun 1.4.2+ on your PATH.

```sh
pnpm install
pnpm exec durable-actors dev
```

Define your actors in `src/durable-objects.ts`. The starter contains a counter with persisted state and `read()` and `increment()` methods. Each counter ID has its own saved value.

`durable-actors dev` watches your actor source and stores local state in `.durable-actors/`. It defaults to project ID `local`; choose another with `pnpm exec durable-actors dev --project-id my-project`. If the CLI is installed globally, you can run `durable-actors dev` directly.

## Connect your application

In your separate application project's directory:

1. Run `pnpm add durable-actors`.
2. Copy the project ID, URL, and shared secret printed by the actor server into your application’s `.env` file.
3. Run `durable-actors generate`; the CLI loads `.env` automatically.

Your application backend can then use the generated client:

```ts
import { actors } from "./generated/index.js"

const counter = actors.Counter.get("example")
console.log(await counter.increment())
```

Start your backend with that `.env` file loaded. Update its secret after restarting the actor server. If you change the actor server's URL or port, also set `DURABLE_ACTORS_CONTROL_PLANE_URL` in the backend environment. After changing actor method signatures, rerun the printed generate command in your application.

Use `pnpm check` to check types and `pnpm build` to create `dist/actors.mjs` for deployment.
