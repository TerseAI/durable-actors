# Local development

Run actors locally using the published npm package. The runtime supplies the API key and control plane URL automatically; no environment variables are needed. For a complete sample application, start with the [Express + React chat example](../../examples/chat/README.md). The steps below cover adding actors to an existing application.

## Install the package

```sh
npm install little-actors
```

The package includes the `little-actors` CLI and TypeScript actor execution support. `little-actors dev` downloads a matching native runtime on first use and caches it for later runs. You do not need Rust or a manually configured binary path.

## Define actors and generate clients

Export your actor classes from `src/durable-objects.ts`, as shown in the [README](../../README.md#define-an-actor). Generate the frontend client and backend proxy together:

```sh
npx little-actors generate
```

Both sides use this output: the frontend imports `clients` and the backend imports `ActorProxy` from `generated/index.js`.

## Start the actor server

```sh
npx little-actors dev
```

Wait for `Local actors ready at http://127.0.0.1:7100`. State is saved in `.little-actors/` and survives restarts.

## Connect your application

Backend actor calls and generated `ActorProxy` helpers use [automatic local configuration](../reference/configuration.md). Start your backend from the same project directory as the actor runtime. Your backend authenticates users and supplies their metadata to `ActorProxy.handle()`. `clients.ChatRoom.get(id)` defaults to `/api/socket/{actorType}/{actorId}` on the current origin.

Start your frontend and application backend with their usual tooling, keeping `little-actors dev` running. The [chat example](../../examples/chat/README.md#run-it) starts Express and React with `npm run dev` and reads the local runtime settings automatically.

The frontend never imports the actor implementation. Generated SDK requests go to your proxy for credentials, and socket messages go directly to the actor gateway.

For remote servers or a custom data directory, see [configuration](../reference/configuration.md).

## Update after changes

Restart `little-actors dev` after changing actor code. Regenerate the shared SDK when the actor contract changes. Local credentials refresh at startup, so your backend must read the current runtime settings after each restart.

## Troubleshooting

If the CLI is missing, run `npm install little-actors` in your application directory before invoking `npx little-actors`. The first actor-server startup needs network access to download the runtime; later runs reuse the cached version.

For server startup, storage, and client connection issues, check the [local defaults and connection settings](../reference/configuration.md).
