# Local development

Run actors locally using the published npm package. The runtime supplies the API key and control plane URL automatically; no environment variables are needed. For a complete sample application, start with the [Express + React chat example](../../examples/chat/README.md). The steps below cover adding actors to an existing application.

## Install the package

```sh
npm install little-actors
```

The package includes the `little-actors` CLI and TypeScript actor execution support. `little-actors dev` downloads a matching native runtime on first use and caches it for later runs. You do not need Rust or a manually configured binary path.

## Define actors and generate clients

Export your actor classes from `src/durable-objects.ts`, as shown in the [README](../../README.md#define-an-actor). Generate the backend RPC and WebSocket grant helpers:

```sh
npx little-actors generate
```

The backend imports `actors` from `generated/index.js`. The frontend fetches a grant from your backend and passes its `websocketUrl` directly to `new WebSocket()`.

## Start the actor server

```sh
npx little-actors dev
```

Wait for `Local actors ready at http://127.0.0.1:7100`. State is saved in `.little-actors/` and survives restarts.

Startup compiles and publishes your actors' public contract. You can then run `npx little-actors generate --url` to generate from the running deployment. Restarting publishes the updated contract under a fresh revision.

The same terminal shows runtime logs and a request log with the method, path, status, and duration when the local control plane responds. Request logs omit query strings, headers, and bodies. Use `RUST_LOG=debug npx little-actors dev` for more detail, or `RUST_LOG=warn npx little-actors dev` to show only warnings and errors.

## Connect your application

Backend actor calls and generated `prepareWebsocket` helpers use [automatic local configuration](../reference/configuration.md). Start your backend from the same project directory as the actor runtime. Your backend authenticates users and calls `actors.ChatRoom.prepareWebsocket({ actorId, metadata })`. The example serves grants at `/api/socket/{actorType}/{actorId}`; your application chooses its own route.

Start your frontend and application backend with their usual tooling, keeping `little-actors dev` running. The [chat example](../../examples/chat/README.md#run-it) starts Express and React with `npm run dev` and reads the local runtime settings automatically.

The frontend never imports the actor implementation. The frontend requests credentials from your backend, then sends socket messages directly to the actor gateway.

For remote servers or a custom data directory, see [configuration](../reference/configuration.md).

## Update after changes

Keep `little-actors dev` running while editing actor code. It watches TypeScript files across the project, recompiles changes in the entrypoint or its imports, and publishes each valid result to the local control plane. Regenerate the backend helpers when the actor contract changes. Invalid intermediate edits leave the last valid contract active. Local credentials refresh at startup, so your backend must read the current runtime settings after each restart.

## Troubleshooting

If the CLI is missing, run `npm install little-actors` in your application directory before invoking `npx little-actors`. The first actor-server startup needs network access to download the runtime; later runs reuse the cached version.

For server startup, storage, and client connection issues, check the [local defaults and connection settings](../reference/configuration.md).
