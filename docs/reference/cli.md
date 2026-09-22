# Command Line Interface

## Create sample templates

```sh
npx durable-actors init chat-example
```

`init <directory>` creates sample templated projects to get started.

- `--template <name>` — Template to copy. Defaults to `chat`.

| Template                                | Description                                                                                                                                                                        |
| --------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `chat`                                  | Express and React chat app with actor definitions, an application authorization route, and native WebSockets.                                                                      |
| `[ai-chat](../../examples/ai-chat)`     | Vercel AI SDK `useChat` over HTTP streaming, with backend actor calls for persistence. Requires [model credentials](../../examples/ai-chat/README.md#run-it). Uses HTTP streaming. |
| `[documents](../../examples/documents)` | Collaborative document editor built on Tiptap and Yjs, with native WebSockets.                                                                                                     |

## Run a development server

```sh
npx durable-actors dev
```

Compiles the actor entrypoint's public contract, registers it with a fresh local deployment revision, and starts the development server. While it runs, it watches TypeScript source throughout the actor project, including files imported by the entrypoint. Each valid change publishes a fresh local revision, so a later `generate --url` reads the updated contract. Invalid intermediate edits are reported without replacing the last valid revision. Uses [environment variables or CLI flags](configuration.md).

- `--api-key <key>` — API key override. `dev` also reads `DURABLE_OBJECT_API_KEY` from `.env`; when neither is set, it generates one, saves it in `<data-dir>/api-key` with owner-only permissions, and prints an export command that reads the file. The key changes on each restart and is not printed.
- `--project <directory>` — Project containing the actor code and installed SDK. Defaults to `.`.
- `--entrypoint <file>` — TypeScript actor source file, relative to the project. Defaults to `src/durable-objects.ts`.
- `--port <number>` — Port for serving local development server
- `--data-dir <directory>` — Folder where data is persisted when developing locally. Defaults to `<project>/.durable-actors`.
- `--storage <backend>` — State and ownership storage, either `local` (default) or `gcs`.

## Open the observability UI

```sh
npx durable-actors observe
```

Verifies admin access to the control plane, starts a local Web UI on an available loopback port, and opens it in your default browser. The React UI checks connectivity through the local server and offers a check/retry button; it does not yet monitor live activity. Control-plane credentials stay on the local server. The terminal prints the UI URL. Press Ctrl+C to stop the server. Failed connection checks print an error and exit with code `1`.

- `--url <origin>`, `--api-key <key>` — [Connection](configuration.md) overrides. Uses `DURABLE_OBJECT_CONTROL_PLANE_URL` when set, otherwise `http://127.0.0.1:7100`.
- `--no-open` — Start the UI and print its URL without launching a browser. If automatic opening fails, the server remains available at the printed URL.

The CLI serves the built UI directly from its `durable-actors-observer` runtime dependency. The UI is also available as the embeddable [`durable-actors-observer` package](../../packages/observer-ui) for hosted and self-hosted applications.

## Inspect saved objects

```sh
npx durable-actors objects list
npx durable-actors objects inspect ChatRoom lobby
```

- `--limit <rows>` — Page size for `list`, from 1 to 500. Defaults to 50.
- `--after <cursor>` — Fetch the page following a cursor. Printed to stderr whenever more objects remain.
- `--all` — Fetch every page. Cannot be combined with `--limit` or `--after`.
- `--json` — Print a JSON array instead of a table, adding snapshot paths and request IDs.
- `--url <origin>`, `--api-key <key>` — [Connection](configuration.md) overrides.

## Build an actor artifact

```sh
npx durable-actors build
```

Checks TypeScript and bundles actor code with its generated schemas into `dist/actors.mjs`. Hosted actors load this artifact without compiling source on startup.

- `[entrypoint]` — TypeScript source file, default `src/durable-objects.ts`.
- `--out-file <file>` — Artifact path, default `dist/actors.mjs`.
- `--config <file>` — TypeScript configuration.

## Deploy actors

```sh
npx durable-actors deploy src/actors.ts --image im-customer-build
```

Sends one deployment request containing the customer build image and source entrypoint. The control plane compiles and publishes the code internally; the deployment terminal needs only the control-plane URL and API key. See [image packaging](../guides/self-hosting.md#4-package-and-deploy-customer-code).

- `[entrypoint]` — Source file inside the build image, default `src/durable-objects.ts`.
- `--image <ref>` — Published customer build image ID.
- `--working-directory <path>` — Project directory inside that image; default `/customer`.
- `--revision <id>` — Optional revision name. Defaults to a new generated ID for each deploy.
- `--secret <name>` — Modal secret reference. Repeatable; uses an on-demand generic sandbox.
- `--url <origin>`, `--api-key <key>` — [Connection](configuration.md) overrides.

## Generate a client and proxy

```sh
npx durable-actors generate
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
- `--api-key <key>` — [Connection](configuration.md) overrides.

### Generate from the control plane

```sh
npx durable-actors generate --url
```

Set `DURABLE_OBJECT_API_KEY` and optionally `DURABLE_OBJECT_CONTROL_PLANE_URL` in the generation terminal. Generation from a published contract writes the same files as source generation and prints the active revision. The server keeps only the latest deployment and its contract. An explicit `--revision` fails if that revision is no longer active.

## Start a hosted server

```sh
npx durable-actors start
```

Starts the packaged server using the [hosted server configuration](configuration.md). It takes no positional arguments or command-specific options, initializes no local project, registers no actor code, and supplies no development credentials. Register code through the [deployment API](http.md#deployments).

### Actor inventory

The observe page lists actor names in the connected deployment with live (resident in memory), dormant, and total instance counts. Deployed types with zero instances remain visible. Unknown counts indicate a live host without a fresh residency report.

When request history is available, the actor list, each actor class page, its instance table, and each instance page also show the **average queue wait**: the mean time admitted requests spent waiting to enter the actor over the selected time range, with the longest single wait and the number of admitted requests. It is computed from `queue_wait_ms` in `request_events`, excludes rerouted attempts and requests that never began processing, and weights class-level averages by each instance's admitted count.

Inventory changes stream from the Rust control plane over SSE, with automatic reconnection and stale-data warnings. Worker residency changes trigger an early host report; socket connections and disconnections publish immediately. Updated Rust hosts, control planes, and SDKs are required. The admin-only stream is `GET /v1/observe/events`; `GET /v1/observe/actors` remains available for single reads. Neither activates actors.

A fifteen-second reconciliation catches missed notifications and lease expiry. Notifications are local to a control-plane process. Socket snapshots are persisted with host leases, so other control-plane processes reconcile the same data. Expired or replaced host sessions cannot contribute connection counts.

### Time ranges

Every history-backed view (the overview, actor queue waits, saved requests, and WebSocket sessions) shares one time range, chosen with the range picker in each page's toolbar and kept as you move between pages. Presets cover the last 5 minutes to the last 7 days plus all retained history; a custom range takes explicit from/to timestamps. Relative ranges are aligned to the minute so polling queries stay stable, and overview metrics for any range are computed in SQL over `request_events` (p95 values use `ROW_NUMBER()` window functions), so a range in the past shows the requests actually recorded then rather than the live 500-event window. Without SQL history the overview falls back to the live window filtered to the range.

### Request timings

The Requests view streams completed method calls and WebSocket lifecycle/message events. Each row shows the actor, operation, request ID, outcome, total duration, and queue wait. Pause freezes the display while collection continues; expand a row to inspect its request, host, and connection IDs.

Total is measured from host submission (or WebSocket message receipt) until actor processing and persistence finish. Queue wait ends when the actor begins processing and includes the per-connection WebSocket message queue. These are host-side timings: they exclude client-side routing, authentication before host submission, and the network round trip. A request rejected or interrupted before processing has no queue-wait value. Retries appear as separate attempts, even when they share a request ID.

`onConnect` events also carry the connection's `metadata` (the value passed to `prepareWebsocket`) when its JSON serialization is at most 4 KiB; larger metadata is omitted from the trace rather than failing it. The WebSockets view pairs each connection's `onConnect` and `onDisconnect` events into a session with its duration, message count, host, and metadata, and marks a session **open** when a host still reports the connection, **closed** when a disconnect was recorded, and **lost** when neither is true (for example after a host restart). Its timeline and table are built from `request_events` through the SQL endpoint below; hovering a connection shows its metadata one field per line so individual users can be told apart, and the filter box suggests actor classes and instance IDs as you type.

Hosts deliver timing records asynchronously; tracing never waits on control-plane delivery in the actor request path. The control plane appends each batch to storage, then wakes the live feed after commit. Each event has a stable UUID, separate from its request ID and the UI's live sequence number. Appending the same retained event again does not duplicate it; distinct attempts sharing a request ID remain distinct events.

The local runtime uses SQLite at `request-traces.sqlite3` inside its state directory (by default `.durable-actors`, overridden with `--data-dir`), including with `--storage gcs`. It retains the latest 10,000 events independently of the 500-event live UI window. Event IDs and replay cursors survive restart. Delivery-loss counters describe the current process and reset on restart. Earlier SQLite schemas and `request-traces.json` snapshots are migrated on open; the original JSON file is preserved. Invalid or unsupported storage fails startup instead of being silently overwritten.

Persistence is injected through the Rust `TracePersistence` trait: `append(events)` commits events, `query({ sql, params })` returns SQL rows, and `replay(...)` supplies the live stream's saved-event cursor. Historical filtering and pagination are owned by the observer UI. The adapter owns SQL execution, retention, deduplication, and live replay. Hosted servers currently use an in-memory SQLite adapter; shared durable cloud storage is not configured yet. A hosted replacement must preserve the public SQL schema and support the UI's SQL dialect and parameter syntax (or adapt those explicitly).

The UI sends raw SQL and bound values to `POST /api/observe/query`; the local CLI forwards them to the admin-only `POST /v1/observe/query` and attaches the API key server-side. Credentials never enter browser JavaScript. Requests contain `{ "sql": "SELECT outcome, COUNT(*) AS total FROM request_events WHERE started_at_ms >= ? GROUP BY outcome", "params": [0] }`; responses contain `{ "rows": [{ "outcome": "completed", "total": 12 }], "truncated": false }`. Values use positional `?` bindings; parameters are JSON strings, numbers, booleans, or null. Result columns must have unique names, and binary results are unsupported.

The public `request_events` view exposes `sequence`, `event_id`, `started_at_ms`, `actor_name`, `actor_id`, `outcome`, `request_id`, `host_id`, `session_id`, `kind`, `operation`, `connection_id`, `duration_ms`, `queue_wait_ms`, and `event` (the original JSON text). `request_history` exposes one row with `generation`, `watermark` (last ingestion position), `pruned` (retention boundary), and `total` (inserted event count). The Requests screen builds SQL for time, actor-ID, and outcome filters and pages by `(started_at_ms, sequence)`, keeping the initial watermark to exclude later appends.

SQL uses SQLite’s authorizer to permit reads and built-in functions in the trace database. There is no custom table or function allowlist. Writes, schema changes, PRAGMAs, attached databases, and extension loading are denied. The public views are the supported UI contract; internal trace tables are also readable. Queries are one statement, at most 16 KiB of SQL and 100 scalar parameters; HTTP bodies are capped at 64 KiB. Results are capped at 500 rows (`truncated` signals more) and 4 MiB; SQLite values are capped at 1 MiB. A progress handler interrupts execution after 500 ms. Queries never execute actor code or access actor state. Hosted reuse additionally requires server-enforced project isolation and restricted database credentials; a tenant filter supplied by browser SQL is not authorization.

The live feed uses the same saved events. An append commits before waking subscribers; notifications only trigger a database read. `GET /v1/observe/requests/events` sends an initial recent page, then events in ingestion order. SSE IDs and `resumeCursor` are opaque replay tokens: reconnect with `after=<resumeCursor>` (or `Last-Event-ID`) to catch up, including late-arriving events. Subscribe-before-read and five-second reconciliation cover missed notifications. If retention or storage replacement invalidates a replay position, `reset` tells the UI to replace its window with available history. This is an SSE feed; no SQLite hook or separate WebSocket transport is required.

Collection is best effort before commit. Failed appends are not published as saved events: they are logged and set `persistenceFailed`, and the UI warns that history may be incomplete. That warning remains for the life of the process. At most 64 batches can await persistence. Overload, disconnection, and process termination can lose records before saving, and an abrupt host exit can lose buffered records without a loss report. Already committed events remain queryable.

The admin-only endpoints are `POST /v1/observe/query` (SQL) and `GET /v1/observe/requests/events` (SSE). Both require updated Rust hosts and control planes; the CLI and observer must also be updated. Neither endpoint executes actor code or changes actor state.
