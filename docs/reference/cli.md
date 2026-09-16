# Command Line Interface

## Create sample templates

```sh
npx little-actors init chat-example
```

`init <directory>` creates sample templated projects to get started.

- `--template <name>` — Template to copy. Defaults to `chat`.

| Template                                | Description                                                                                                                                                       |
| --------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `chat`                                  | Express and React chat app with actor definitions, a socket-ticket route, and generated WebSocket clients.                                                        |
| `[ai-chat](../../examples/ai-chat)`     | Vercel AI SDK `useChat` over HTTP streaming, with backend actor calls for persistence. Requires `OPENAI_API_KEY` in `.env`. Needs no generated WebSocket clients. |
| `[documents](../../examples/documents)` | Collaborative document editor built on Tiptap and Yjs, with generated WebSocket clients.                                                                          |

## Run a development server

```sh
npx little-actors dev
```

Loads the actor entrypoint, registers your actors, and serves the development web server

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
- `--url <origin>`, `--api-key <key>` — [Cloud connection](#connect-to-a-cloud-control-plane) overrides.

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
- `--url <origin>`, `--api-key <key>`, `--namespace <id>` — [Cloud connection](#connect-to-a-cloud-control-plane) overrides.

## Generate a client and proxy

```sh
npx little-actors generate
```

Checks the actor dependency graph without executing it and writes TypeScript clients.

| File         | Contents                                                                |
| ------------ | ----------------------------------------------------------------------- |
| `index.ts`   | Browser `ActorClient` and actor WebSocket descriptors.                  |
| `proxy.ts`   | Backend `ActorProxy` for authorizing browser connections.               |
| `backend.ts` | Typed RPC descriptors such as `ChatRoom.get("lobby").sendMessage(...)`. |

The source entrypoint is a positional argument and defaults to `src/durable-objects.ts`.

- `--out-dir <directory>` — Output location. Defaults to `generated/`.
- `--config <file>` — TypeScript configuration. Cannot be combined with `--url`.
- `--url [origin]` — Generate from a published contract instead of local source. Takes the [cloud connection](#connect-to-a-cloud-control-plane) origin when given no value. Cannot be combined with a source entrypoint or `--config`.
- `--revision <id>` — Optional check that the active deployment matches this revision. Defaults to the latest deployment's contract.
- `--api-key <key>`, `--namespace <id>` — [Cloud connection](#connect-to-a-cloud-control-plane) overrides.

### Generate from the control plane

```sh
npx little-actors generate --url
```

Remote generation writes the same files as local generation and prints the active revision. The server keeps only the latest deployment and its contract. An explicit `--revision` fails if that revision is no longer active.

## Start a hosted server

```sh
npx little-actors start
```

Starts the packaged server using self-hosting settings from the environment. It takes no positional arguments or command-specific options, initializes no local project, registers no actor code, and supplies no development credentials. Register code through the [deployment API](http.md#deployments).

## Environment variables

| Variable                          | Default                            | Description                                                                                                                                          |
| --------------------------------- | ---------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DURABLE_OBJECT_BINARY`           | Downloaded runtime                 | Path to an existing native executable for `dev` and `start`, bypassing runtime downloads. Relative paths resolve from the current working directory. |
| `DURABLE_OBJECT_CACHE_DIR`        | `~/.cache/little-actors`           | Root directory for downloaded runtimes. Ignored when `DURABLE_OBJECT_BINARY` is set.                                                                 |
| `DURABLE_OBJECT_STANDARD_BUCKETS` | none; required for `--storage gcs` | JSON object mapping storage regions to bucket names. See [GCS snapshots](#save-snapshots-in-gcs).                                                    |
| `GOOGLE_APPLICATION_CREDENTIALS`  | ADC discovery                      | Service-account credentials file for GCS. An attached Google identity can also supply credentials.                                                   |
| `RUST_LOG`                        | `info`                             | Runtime log filter, for example `warn` or `debug`.                                                                                                   |

## Runtime installation

```sh
npm install little-actors
```

The package includes the SDK, CLI, templates, and TypeScript actor execution support. `dev` and `start` download a native runtime matching the installed package version if it is not cached, verified against the release's SHA-256 checksum. `init`, `deploy`, `generate`, `token`, `objects`, and help do not download a runtime.

Prebuilt platforms are macOS and Linux on ARM64 and x64. Linux requires glibc 2.35+ and OpenSSL 3, such as Ubuntu 22.04+. Windows users can run the Linux distribution in WSL 2.

Runtimes are cached at `~/.cache/little-actors/<version>/<platform>-<arch>/`.

## Issue a local token

```sh
npx little-actors token
```

Requests a session token from the running local server, reading its origin from the data directory. Standard output contains only the token followed by a newline; errors go to standard error.

- `--data-dir <directory>` — Directory belonging to the running local server. Defaults to `.little-actors`, relative to the current directory.

The requested deadline is one hour in the future. Issuance adds up to 30 seconds of grace, subject to the server's lifetime cap. Regenerate the token after a server restart.

The token grants application access throughout the `local` namespace: not an admin credential, and not restricted to one room. See [session tokens](http.md#session-tokens) for scope and expiration rules. `token` is a diagnostic command for trusted backend tools; browser SDKs obtain actor-scoped tickets through your authenticated proxy.

### Connect with a WebSocket tool

```sh
TOKEN="$(npx little-actors token)"
npx --yes wscat \
    -c ws://127.0.0.1:7100/v1/namespaces/local/actors/ChatRoom/lobby/websocket \
    -H "Authorization: Bearer $TOKEN" \
    -x '{"type":"initialize","metadata":{}}' \
    -w -1
```

Use the server's actual port. The [WebSocket reference](http.md#direct-websocket-connections) describes initialization and message formats.
