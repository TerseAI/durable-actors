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

Compiles the actor entrypoint's public contract, registers it with a fresh local deployment revision, and starts the development server. While it runs, it watches TypeScript source throughout the actor project, including files imported by the entrypoint. Each valid change publishes a fresh local revision, so a later `generate --url` reads the updated contract. Invalid intermediate edits are reported without replacing the last valid revision. Uses [automatic local configuration](configuration.md).

- `--project <directory>` — Project containing the actor code and installed SDK. Defaults to `.`.
- `--entrypoint <file>` — TypeScript actor source file, relative to the project. Defaults to `src/durable-objects.ts`.
- `--port <number>` — Port for serving local development server
- `--data-dir <directory>` — Folder where data is persisted when developing locally. Defaults to `<project>/.little-actors`.
- `--storage <backend>` — Snapshot storage, either `local` (default) or `gcs`.

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
- `--data-dir <directory>` — State directory of the running local server. Defaults to `.little-actors`.
- `--url <origin>`, `--api-key <key>` — [Connection](configuration.md) overrides.

## Deploy actors

```sh
npx little-actors deploy --image im-chat --working-directory /app
```

Registers a built image plus the public API contract extracted from the TypeScript source

- `--image <ref>` — Provider image to register.
- `--revision <id>` — Optional revision name. Defaults to a new generated ID for each deploy.
- `--working-directory <path>` — Absolute project path inside the image.
- `--actor-entrypoint <path>` — Entrypoint inside the image.
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
- `--url [origin]` — Generate from a published contract instead of local source. With no value, uses the running local runtime or [connection](configuration.md) overrides. Cannot be combined with a source entrypoint or `--config`.
- `--revision <id>` — Optional check that the active deployment matches this revision. Defaults to the latest deployment's contract.
- `--api-key <key>`, `--namespace <id>` — [Connection](configuration.md) overrides.

### Generate from the control plane

```sh
npx little-actors generate --url
```

With `little-actors dev` running, this reads the local runtime's URL and API key automatically. Generation from a published contract writes the same files as source generation and prints the active revision. The server keeps only the latest deployment and its contract. An explicit `--revision` fails if that revision is no longer active.

## Start a hosted server

```sh
npx little-actors start
```

Starts the packaged server using the [hosted server configuration](configuration.md). It takes no positional arguments or command-specific options, initializes no local project, registers no actor code, and supplies no development credentials. Register code through the [deployment API](http.md#deployments).

## Issue a local token

```sh
npx little-actors token
```

Requests a session token from the running local server, reading its origin from the data directory. Standard output contains only the token followed by a newline; errors go to standard error.

- `--data-dir <directory>` — Directory belonging to the running local server. Defaults to `.little-actors`, relative to the current directory.

The requested deadline is one hour in the future. Issuance adds up to 30 seconds of grace, subject to the server's lifetime cap. Regenerate the token after a server restart.

The token grants application access throughout the `local` namespace: not an admin credential, and not restricted to one room. See [session tokens](http.md#session-tokens) for scope and expiration rules. `token` is a diagnostic command for trusted backend tools; browsers obtain actor-scoped URLs and keys through your authenticated backend.
