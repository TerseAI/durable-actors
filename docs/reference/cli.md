# Command Line Interface

## Create sample templates

```sh
npx little-actors init chat-example
```

`init <directory>` creates sample templated projects to get started.

- `--template <name>` — Template to copy. Defaults to `chat`.

| Template                                | Description                                                                                                                                                                        |
| --------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `chat`                                  | Express and React chat app with actor definitions, a socket-ticket route, and native WebSockets.                                                                                   |
| `[ai-chat](../../examples/ai-chat)`     | Vercel AI SDK `useChat` over HTTP streaming, with backend actor calls for persistence. Requires [model credentials](../../examples/ai-chat/README.md#run-it). Uses HTTP streaming. |
| `[documents](../../examples/documents)` | Collaborative document editor built on Tiptap and Yjs, with native WebSockets.                                                                                                     |

## Run a development server

```sh
npx little-actors dev
```

Compiles the actor entrypoint's public contract, registers it with a fresh local deployment revision, and starts the development server. While it runs, it watches TypeScript source throughout the actor project, including files imported by the entrypoint. Each valid change publishes a fresh local revision, so a later `generate --url` reads the updated contract. Invalid intermediate edits are reported without replacing the last valid revision. Uses [environment variables or CLI flags](configuration.md).

- `--api-key <key>` — API key override. `dev` also reads `DURABLE_OBJECT_API_KEY` from `.env`; when neither is set, it generates one and prints an export command.
- `--project <directory>` — Project containing the actor code and installed SDK. Defaults to `.`.
- `--entrypoint <file>` — TypeScript actor source file, relative to the project. Defaults to `src/durable-objects.ts`.
- `--port <number>` — Port for serving local development server
- `--data-dir <directory>` — Folder where data is persisted when developing locally. Defaults to `<project>/.little-actors`.
- `--storage <backend>` — State and ownership storage, either `local` (default) or `gcs`.

## Inspect saved objects

```sh
npx little-actors objects list
npx little-actors objects inspect ChatRoom lobby
```

- `--namespace <id>` — Restrict to one namespace. Listing covers all namespaces unless this is set. Local dev defaults to `local`.
- `--limit <rows>` — Page size for `list`, from 1 to 500. Defaults to 50.
- `--after <cursor>` — Fetch the page following a cursor. Printed to stderr whenever more objects remain.
- `--all` — Fetch every page. Cannot be combined with `--limit` or `--after`.
- `--json` — Print a JSON array instead of a table, adding snapshot paths and request IDs.
- `--url <origin>`, `--api-key <key>` — [Connection](configuration.md) overrides.

## Build an actor artifact

```sh
npx little-actors build
```

Checks TypeScript and bundles actor code with its generated schemas into `dist/actors.mjs`. Hosted actors load this artifact without compiling source on startup.

- `[entrypoint]` — TypeScript source file, default `src/durable-objects.ts`.
- `--out-file <file>` — Artifact path, default `dist/actors.mjs`.
- `--config <file>` — TypeScript configuration.

## Deploy actors

```sh
npx little-actors deploy --image im-chat --working-directory /app --actor-entrypoint dist/actors.mjs
```

Registers a built image plus the public API contract extracted from the TypeScript source

- `--image <ref>` — Provider image to register.
- `--revision <id>` — Optional revision name. Defaults to a new generated ID for each deploy.
- `--working-directory <path>` — Absolute project path inside the image.
- `--actor-entrypoint <path>` — Entrypoint inside the image. Use `dist/actors.mjs` for the build artifact; this is also the default.
- `--config <file>` — TypeScript configuration for extraction.
- `--secret <name>` — Provider secret reference. Repeatable.
- `--socket-gateway-url <origin>` — Separate socket gateway.
- `--warm-region <region>` — Background image warmup.
- `--url <origin>`, `--api-key <key>`, `--namespace <id>` — [Connection](configuration.md) overrides.

## Generate a client and proxy

```sh
npx little-actors generate
```

Checks the actor dependency graph without executing it and writes TypeScript backend helpers.

| Entrypoint | Export            | Contents                                                                                             |
| ---------- | ----------------- | ---------------------------------------------------------------------------------------------------- |
| `index.ts` | `actors.ChatRoom` | Typed RPC client via `.get(id)` and WebSocket grants via `.prepareWebsocket({ actorId, metadata })`. |
| `index.ts` | `ActorProxy`      | Authorization proxy and metadata types for browser connections.                                      |

Regeneration removes the old per-actor files and frontend, backend, and proxy entrypoints. Import backend helpers from `generated/index.js`. Frontends use native `WebSocket` without generated imports.

The source entrypoint is a positional argument and defaults to `src/durable-objects.ts`.

- `--out-dir <directory>` — Output location. Defaults to `generated/`.
- `--config <file>` — TypeScript configuration. Cannot be combined with `--url`.
- `--url [origin]` — Generate from a published contract instead of local source. With no value, uses `DURABLE_OBJECT_CONTROL_PLANE_URL` or `http://127.0.0.1:7100`. Cannot be combined with a source entrypoint or `--config`.
- `--revision <id>` — Optional check that the active deployment matches this revision. Defaults to the latest deployment's contract.
- `--api-key <key>`, `--namespace <id>` — [Connection](configuration.md) overrides.

### Generate from the control plane

```sh
npx little-actors generate --url
```

Set `DURABLE_OBJECT_API_KEY` and optionally `DURABLE_OBJECT_CONTROL_PLANE_URL` in the generation terminal. Generation from a published contract writes the same files as source generation and prints the active revision. The server keeps only the latest deployment and its contract. An explicit `--revision` fails if that revision is no longer active.

## Start a hosted server

```sh
npx little-actors start
```

Starts the packaged server using the [hosted server configuration](configuration.md). It takes no positional arguments or command-specific options, initializes no local project, registers no actor code, and supplies no development credentials. Register code through the [deployment API](http.md#deployments).

## Issue a local token

```sh
npx little-actors token
```

Requests a session token using connection flags or environment variables. Standard output contains only the token followed by a newline; errors go to standard error.

- `--url <origin>`, `--api-key <key>`, `--namespace <id>` — [Connection](configuration.md) settings.
- `--region <region>` — Execution region, or `DURABLE_OBJECT_REGION`; defaults to `north-america-east`.

The requested deadline is one hour in the future. Issuance adds up to 30 seconds of grace, subject to the server's lifetime cap. Regenerate the token after a server restart.

The token grants application access throughout the `local` namespace: not an admin credential, and not restricted to one room. See [session tokens](http.md#session-tokens) for scope and expiration rules. `token` is a diagnostic command for trusted backend tools; browsers obtain actor-scoped URLs and keys through your authenticated backend.
