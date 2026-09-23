# Actors

Local actor development requires Node.js 22.19.0+, pnpm, and Bun 1.3.9+ on your PATH. This actor project uses a higher Node.js minimum than the SDK: applications that only consume the SDK or generate clients follow its declared range, `^20.19.0 || >=22.12.0` (Node.js 20.x from 20.19, or 22.12 and newer).

```sh
pnpm install
pnpm exec durable-actors dev
```

Define your actors in `src/actors.ts`. The starter contains a counter with persisted state and `read()` and `increment()` methods. Each counter ID has its own saved value.

`pnpm exec durable-actors dev` watches your actor source and stores local state in `.durable-actors/`. It defaults to project ID `local`; choose another with `DURABLE_ACTORS_PROJECT_ID=my-project pnpm exec durable-actors dev`.

## Connect your application

In your separate application project's directory:

1. Run `pnpm add -D durable-actors` or `npm install --save-dev durable-actors`.
2. Use the local defaults: project `local`, URL `http://127.0.0.1:7100`, and no secret. If you configure a different project, port, or optional secret on the actor server, put matching settings in your application’s `.env` or `.env.local` file.
3. Run `pnpm exec durable-actors generate` or `npx durable-actors generate`; the CLI loads both environment files automatically. Exported environment variables take precedence over `.env.local`, which takes precedence over `.env`.

Your application backend can then use the generated client:

```ts
import { actors } from "./generated/index.js"

const counter = actors.Counter.get("example")
console.log(await counter.increment())
```

Start your backend with any configured environment file loaded. Local development requires no secret by default. An explicitly configured `DURABLE_ACTORS_SECRET` stays the same across actor server restarts; update the backend only when you change the server’s configured secret. If you change the actor server's URL or port, also set `DURABLE_ACTORS_CONTROL_PLANE_URL` in the backend environment. After changing actor method signatures, rerun the generate command in your application.

Use `pnpm check` to check types. Production deployment integrations register actor images through `PUT /v1/projects/{project_id}/deployment`; see the [HTTP API](https://github.com/TerseAI/durable-actors/blob/main/docs/reference/openapi.yaml).
