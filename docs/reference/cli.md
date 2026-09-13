# Command Line Interface

- [Create a chat app](#create-a-chat-app)
- [Find your actors](#find-your-actors)
- [Run the development server](#run-the-development-server)
- [Inspect saved objects](#inspect-saved-objects)
- [Generate a client and proxy](#generate-a-client-and-proxy)
- [Start a hosted server](#start-a-hosted-server)
- [Environment variables](#environment-variables)
- [Runtime installation](#runtime-installation)
- [Issue a local token](#issue-a-local-token)
- [Help and version](#help-and-version)
- [Output and exit codes](#output-and-exit-codes)
- [Troubleshooting](#troubleshooting)

## Create a chat app

```sh
npx little-actors init chat-example
```

`init <directory>` creates an Express + React chat app from the template bundled in the npm package. It refuses an existing destination. The parent directory must exist. Once the package is installed, copying the template needs no network access.

The app includes actor definitions, a socket-ticket route, and a React interface. Its `little-actors` dependency matches the CLI version. The command prints the next steps: install dependencies, generate the client and proxy once, then start the actor server and app. It does not install dependencies or start processes itself.

Use `--template ai-chat` for [Vercel AI SDK with durable chat history](../../examples/ai-chat):

```sh
npx little-actors init ai-chat-example --template ai-chat
```

This template uses `useChat` and HTTP streaming, with backend actor calls for persistence. It needs an `OPENAI_API_KEY` in `.env` and does not need generated WebSocket clients.

Use `--template documents` for the [collaborative document editor](../../examples/documents):

```sh
npx little-actors init documents-example --template documents
```

This template combines Tiptap, Yjs, and generated WebSocket clients. `--template` accepts `chat` (the default), `ai-chat`, or `documents`.

## Find your actors

The development server looks for `src/durable-objects.ts` in the current directory. The file must exist before startup and export named actor classes. See [actor definitions](api.md#actor) for class and export requirements.

Select a different project or actor file with `--project` and `--entrypoint`:

```sh
npx little-actors dev --project ./chat-example --entrypoint src/actors.ts
```

The entrypoint resolves relative to the project. In this example, the server loads `chat-example/src/actors.ts`. The project must have `little-actors` installed. Source loading validates field annotations automatically; see [explicit persistence](api.md#saved-state-and-serialization).

## Run the development server

```sh
npx little-actors dev
```

Starts a server on IPv4 loopback, loads the actor entrypoint, and registers your actors. Keep it running while using clients. Wait for the ready message before connecting:

```text
Local actors ready at http://127.0.0.1:7100
```

Local mode needs no cloud credentials, database URL, bucket, or signing key. SQLite metadata and snapshots are saved in `.little-actors/` by default.

### dev options

```text
little-actors dev [options]
```

- `--project <directory>` — Project containing the actor code and installed SDK. Defaults to `.`.
- `--entrypoint <file>` — TypeScript actor source file relative to the project. Defaults to `src/durable-objects.ts`.
- `--port <number>` — Loopback port, an integer from `0` through `65535`. Defaults to `7100`; `0` selects an available port.
- `--data-dir <directory>` — Persistent state and connection settings. Defaults to `<project>/.little-actors`. An explicit relative path resolves from the shell's working directory.
- `--storage <backend>` — Snapshot storage, either `local` (default) or `gcs`. Local metadata stays in the data directory for both backends.
- `-h`, `--help` — Print command help.

### Choose a port

```sh
npx little-actors dev --port 7200
```

Use `--port 0` to select an available port. The ready message prints the selected origin; `token` reads it automatically from the data directory. Configure your application proxy using `.little-actors/runtime.json`.

### Keep state across restarts

```sh
npx little-actors dev --data-dir ./chat-state
```

Keep the entire data directory to preserve actors across restarts. Only one `dev` process can use it at a time. Separate projects can run with different data directories and ports.

Stop with Ctrl-C. Actor code changes require restarting `dev`; there is no file watcher. Connection settings and local credentials are refreshed at startup, so refresh the proxy credentials and reconnect the web app after restarting.

Local storage is intended for development. Deleting the directory or losing its machine loses the saved actors. A fresh directory starts a fresh local environment.

### Save snapshots in GCS

```sh
export DURABLE_OBJECT_STANDARD_BUCKETS='{"north-america-east":"my-actor-state-bucket"}'
export GOOGLE_APPLICATION_CREDENTIALS='/absolute/path/to/service-account.json'
npx little-actors dev --storage gcs --data-dir .gcs-demo
```

GCS mode uses Google Application Default Credentials and a nonempty region-to-bucket map. Bucket values are names without `gs://` or slashes. Local metadata still lives in the data directory and must be preserved.

Changing the storage backend or bucket map requires a separate data directory; startup rejects a changed configuration for an existing directory. See [local execution with GCS](../guides/self-hosting.md#local-execution-with-gcs) for storage and credential setup.

## Inspect saved objects

```sh
npx little-actors objects list
npx little-actors objects list --namespace local
npx little-actors objects list --namespace local --limit 20
npx little-actors objects inspect ChatRoom lobby
```

`objects list` shows up to 50 saved objects across all namespaces on the connected server, with their namespace, actor type, ID, committed version, and storage region. `--namespace <id>` filters the list. Objects appear after their first successful state commit, including objects whose hosts have stopped or whose deployment has been removed. `--json` prints a JSON array with snapshot paths and request IDs as well.

Use `--limit <rows>` to choose a page size from 1 to 500. When more objects remain, the CLI prints an `--after` cursor to stderr. Repeat the same command with that cursor to get the next page, keeping the namespace and connection options:

```sh
npx little-actors objects list --namespace local --limit 20 --after 'CURSOR_FROM_PREVIOUS_PAGE'
```

`--all` fetches every page automatically. It cannot be combined with `--limit` or `--after`. Pagination hints stay on stderr so `--json` output remains a valid JSON array. To scroll the full table in a terminal pager, use `npx little-actors objects list --all | less -S`; press `q` to exit.

`objects inspect <actor-type> <actor-id>` prints JSON containing the object's identity, committed version, snapshot path, last request ID, and `state`. This includes all `@Persisted` fields, including internal fields. `@Ephemeral` fields are not stored. Inspection reads the snapshot referenced by the committed database record, without starting the actor or running its code. Concurrent writes may commit a newer version after that record is read.

An existing object with no committed snapshot returns `stateVersion: 0` and `state: null`. An unknown object returns an error.

### Local inspection

Keep `little-actors dev` running. The commands read its URL, API key, and default namespace from `.little-actors/runtime.json`. Use `--data-dir <directory>` for a custom state directory:

```sh
npx little-actors objects inspect ChatRoom lobby --data-dir ./chat-state
```

This works with both local snapshots and `dev --storage gcs`.

### Cloud inspection

Set the control-plane origin and its admin API key:

```sh
export DURABLE_OBJECT_CONTROL_PLANE_URL='https://objects.example.com'
export DURABLE_OBJECT_API_KEY='<your-api-key>'
npx little-actors objects list
npx little-actors objects list --namespace my-project --json
npx little-actors objects inspect ChatRoom lobby --namespace my-project
```

Both commands also accept `--url <origin>` and `--api-key <key>`, which override the corresponding environment variables. A remote URL requires a remote API key; local credentials are never used as a fallback. These commands require a server version that provides the [object inspection API](http.md#object-inspection).

For inspection, `--namespace` overrides the connection's default: `local` for local dev, or `DURABLE_OBJECT_NAMESPACE_ID` (otherwise `default`) for cloud. Listing always covers all namespaces unless `--namespace` is supplied. Cloud URL or API-key settings select the cloud connection instead of `--data-dir`.

Inspection requires the admin API key. Session tokens and browser socket tickets cannot read saved internal state. These commands do not download a native runtime.

## Generate a client and proxy

```sh
npx little-actors generate
```

The entrypoint defaults to `src/durable-objects.ts`; output defaults to `generated/`. Pass a different entrypoint as a positional argument, `--out-dir <directory>` to change the output location, or `--config <file>` to select a TypeScript configuration. Generation checks the actor dependency graph without executing it and fails immediately on unsupported socket or public-state types.

Output contains TypeScript descriptors, `ActorClient` in `index.ts`, and `ActorProxy` in `proxy.ts`. Generate these source files once in a shared directory, or copy the output into separate projects. Each project needs `little-actors` installed. Import `index.ts` from the frontend and `proxy.ts` from the backend. The proxy restricts actor names at runtime and metadata types at compile time. Actor-specific runtime validation runs in the actor host. Regeneration removes obsolete validator files for the generated actors. This does not publish a separate SDK package. Regenerate when the actor contract changes. See [browser clients](../../sdk/README.md#browser-clients) for the proxy and frontend integration.

## Start a hosted server

```sh
npx little-actors start
```

Starts the packaged server using self-hosting settings from the environment. It takes no positional arguments or command-specific options beyond `-h` / `--help`.

Configure the database, storage, authentication, and cloud provider using the [self-hosting guide](../guides/self-hosting.md). Every server setting and default is listed under [server configuration](../guides/self-hosting.md#server-configuration). The npm CLI defaults `DURABLE_OBJECT_PROCESS_ROLE` to `control_plane`; leave it unset for this use.

`start` does not initialize a local project, register actor code, or supply development credentials. Register code separately using the [deployment API](http.md#deployments).

## Environment variables

Local actor processes inherit ordinary application environment variables from the development server process. Hosted server settings are listed in [server configuration](../guides/self-hosting.md#server-configuration); SDK client settings are in [client configuration](api.md#client-configuration).

### DURABLE_OBJECT_BINARY

**Default:** Downloaded runtime.

Optional path to an existing native executable used by `dev` and `start`. Relative paths resolve from the current working directory. This bypasses runtime downloads. Normal npm installs select and download the runtime automatically.

### DURABLE_OBJECT_CACHE_DIR

**Default:** `~/.cache/little-actors`.

Root directory for downloaded runtimes. Ignored when `DURABLE_OBJECT_BINARY` is set.

### DURABLE_OBJECT_STANDARD_BUCKETS

**Required for `--storage gcs`.** JSON object mapping storage regions to bucket names. It is not needed for `--storage local`. See [GCS snapshots](#save-snapshots-in-gcs).

### GOOGLE_APPLICATION_CREDENTIALS

**Default:** Google Application Default Credentials discovery.

Service-account credentials file for GCS. An attached Google identity can also supply credentials.

### RUST_LOG

**Default:** `info`.

Runtime log filter, for example `warn` or `debug`.

## Runtime installation

Install the package before invoking its `little-actors` executable:

```sh
npm install little-actors
npx little-actors --help
```

The package includes the SDK, CLI, chat template, and TypeScript actor execution support. `dev` and `start` download a native runtime matching the installed package version if it is not cached. Downloads are verified against the release's SHA-256 checksum; `init`, `generate`, `token`, `objects`, and help do not download a runtime.

Prebuilt platforms are macOS and Linux on ARM64 and x64. Linux requires glibc 2.35+ and OpenSSL 3, such as Ubuntu 22.04+. Windows users can run the Linux distribution in WSL 2.

A prebuilt runtime does not require Rust or a manually configured binary path.

The default cache path is `~/.cache/little-actors/<version>/<platform>-<arch>/`. `DURABLE_OBJECT_CACHE_DIR` changes the download cache location.

## Issue a local token

```sh
npx little-actors token
```

Requests a session token from the running local server. Standard output contains only the token followed by a newline. Errors go to standard error.

### token options

```text
little-actors token [options]
```

- `--data-dir <directory>` — Directory belonging to the running local server. Defaults to `.little-actors`, relative to the current directory.
- `-h`, `--help` — Print command help.

The requested deadline is one hour in the future. Token issuance adds up to 30 seconds of grace, subject to the server's lifetime cap. Regenerate the token after a server restart.

This diagnostic command is for trusted backend tools. Browser SDKs obtain actor-scoped tickets through your authenticated proxy instead.

The token permits application access throughout the `local` namespace. It is neither an admin credential nor restricted to one room. See [session tokens](http.md#session-tokens) for scope and expiration rules.

### Connect with a WebSocket tool

```sh
TOKEN="$(npx little-actors token)"
npx --yes wscat \
    -c ws://127.0.0.1:7100/v1/namespaces/local/actors/ChatRoom/lobby/websocket \
    -H "Authorization: Bearer $TOKEN" \
    -x '{"type":"initialize","metadata":{}}' \
    -w -1
```

Use the server's actual port, and pass `--data-dir` to `token` if the server uses a custom directory. The [WebSocket reference](http.md#direct-websocket-connections) describes initialization and message formats.

## Help and version

```sh
npx little-actors --help
npx little-actors dev --help
npx little-actors --version
```

- `-h`, `--help` — Print help. Available on the root command and each subcommand.
- `-V`, `--version` — Print the npm package version. Available on the root command.

Invoking the npm CLI without a command prints help. There is no separate `help` command.

The commands on this page use the npm CLI. The native executable supports `dev`, `--help`, and `--version`; `init`, `generate`, `token`, `objects`, and `start` are npm CLI commands.

## Output and exit codes

`dev` writes the ready origin, state directory, and runtime logs. `token` writes its credential to stdout and errors to stderr. `objects list` writes a table, or a JSON array with `--json`; `objects inspect` writes JSON. Inspection errors go to stderr and exit with code 1. Help and version commands exit successfully.

Setup and argument errors return a nonzero exit code, normally `1`. `dev` and `start` forward the child process's numeric exit code. A child terminated by SIGINT maps to `130`; another terminating signal maps to `1`.

Ctrl-C requests shutdown. It does not erase actor state.

## Troubleshooting

### No local runtime found

Start `dev` and use its data directory for `token`. Your application proxy needs the current backend URL and API key. With a custom directory or a different working directory, pass `--data-dir` explicitly.

### Cannot reach the local runtime

Restart `dev`, wait for the ready message, and refresh the proxy credentials. Tokens copied before the restart must be regenerated.

### Actor file is missing or changes do not appear

Create the entrypoint before starting `dev`, or select it with `--entrypoint`. Restart `dev` after actor code changes, and regenerate clients if the actor contract changed.

### Port or data directory is in use

Choose another `--port`, or use `--port 0`. Stop the runtime using the data directory or choose a separate directory.

### Storage configuration changed

Use a separate `--data-dir` for a different backend or bucket map. Existing actors are not migrated by changing these settings.

### Runtime download fails

Check your network connection and confirm that the installed package version has matching native release assets. For a checksum mismatch, retry the download; the rejected archive is not usable.

For application setup, see [local development](../guides/local-development.md).
