# Local development

Run actors locally using the published npm package. For a complete sample application, start with the [Express + React chat example](../../examples/chat/README.md).

## Install the package

Install Node.js 20+ and Bun 1.4.2+ on your PATH. The CLI uses Node; Rust starts Bun to execute each actor in a separate process.

```sh
npm install little-actors
```

The package includes the `little-actors` CLI and TypeScript actor execution support. `little-actors start --dev` downloads a matching native runtime on first use and caches it for later runs. You do not need Rust or a manually configured binary path.

## Define actors and generate clients

Export your actor classes from `src/durable-objects.ts`, as shown in the [README](../../README.md#define-an-actor). Generate the backend RPC and WebSocket grant helpers:

```sh
npx little-actors generate
```

The backend imports `actors` from `generated/index.js`. The frontend fetches a grant from your backend and passes its `websocketUrl` directly to `new WebSocket()`.

## Start the actor server

```sh
npx little-actors start --dev --project-id my-project
```

Wait for the `Ready` line. State is saved in `.little-actors/` and survives restarts. You can also set `DURABLE_OBJECT_PROJECT_ID` in `.env` instead of passing `--project-id`.

Startup compiles and publishes your actors' public contract. You can then run `npx little-actors generate --url` to generate from the running deployment. Restarting publishes the updated contract under a fresh revision.

The same terminal shows runtime logs and a request log with the method, path, status, and duration when the local control plane responds. Request logs omit query strings, headers, and bodies. Use `RUST_LOG=debug npx little-actors start --dev` for more detail, or `RUST_LOG=warn npx little-actors start --dev` to show only warnings and errors.

## Connect your application

Set the same `DURABLE_OBJECT_PROJECT_ID` in your application backend. If `start --dev` generates a key, run the printed `export DURABLE_OBJECT_API_KEY=…` command in your backend terminal. Backend actor calls and generated `prepareWebsocket` helpers read those settings.

Start your frontend and application backend with their usual tooling, keeping `little-actors start --dev` running.

The frontend never imports the actor implementation. The frontend requests credentials from your backend, then sends socket messages directly to the actor gateway.

For remote servers or a custom data directory, see [configuration](../reference/configuration.md).

## Update after changes

Keep `little-actors start --dev` running while editing actor code. Restarting it generates a new key unless you provide one explicitly.

## Test Modal hosts against a local control plane

Modal hosts need to reach your control plane over the internet. From this repository, `pnpm run start:cloud` starts an ngrok HTTPS endpoint and then your control plane. The tunnel forwards to `127.0.0.1:7100` by default. It uses HTTP/2 upstream forwarding because the control plane serves gRPC as well as HTTP. See the [ngrok CLI reference](https://ngrok.com/docs/agent/cli).

Install the [ngrok CLI](https://ngrok.com/download), then add your agent authtoken to the repository's ignored `.env` file:

```dotenv
NGROK_AUTH_TOKEN=your-ngrok-authtoken
# Optional: use a domain assigned to your ngrok account.
NGROK_DOMAIN=your-domain.ngrok.app
```

Omit `NGROK_DOMAIN` to let ngrok choose the URL. The domain may also include `https://`. The helper also accepts `NGROK_AUTHTOKEN` and `NGROK_URL` as fallbacks, or an authtoken already saved in your ngrok configuration. It loads `.env` from the current directory; exported shell variables take precedence. It does not need an ngrok API key.

Configure the [self-hosting settings](../reference/configuration.md) in `.env`: `DURABLE_OBJECT_SANDBOX_PROVIDER=modal`, the shared runtime image ID, both `MODAL_TOKEN_ID` and `MODAL_TOKEN_SECRET`, the API key, JWT signing key, PostgreSQL URL, GCS bucket, and Google credentials. Build once, then start both processes from the repository root:

```sh
pnpm run build
pnpm run start:cloud
```

The command waits for the tunnel to be ready, saves `DURABLE_OBJECT_CONTROL_PLANE_URL=https://...` in `.env`, and starts the control plane with that URL. Other settings and comments are preserved; `.env` is created if needed. The discovered URL overrides any older shell export for the launched control plane. Ctrl+C stops both processes. If either process exits, the other is stopped too.

For a quick check in another terminal, use the built CLI:

```sh
node sdk/dist/cli.js actors list
node sdk/dist/cli.js actors inspect Counter YOUR_ACTOR_ID
```

You can still run `pnpm run tunnel` and `pnpm run start` separately. In that case, wait for the tunnel before starting the control plane; unset any older shell export of `DURABLE_OBJECT_CONTROL_PLANE_URL` so `.env` takes precedence. A running process or another terminal's environment cannot be changed by the helper.

If you change `DURABLE_OBJECT_CONTROL_PLANE_BIND`, the tunnel forwards to that address and port instead; wildcard addresses use loopback. `start` runs the hosted control plane locally and provisions actors on Modal. `little-actors start --dev` always uses local actor processes, even when Modal credentials are set.

Follow the [self-hosting guide](self-hosting.md#4-package-and-deploy-customer-code) to package and register your Modal actor image. Use the printed public URL and the same API key in your deployment and application backend terminals. If the tunnel URL changes, restart the control plane and replace existing Modal hosts so they receive the new callback address.

## Troubleshooting

If the CLI is missing, run `npm install little-actors` in your application directory before invoking `npx little-actors`. The first actor-server startup needs network access to download the runtime; later runs reuse the cached version.

For server startup, storage, and client connection issues, check the [local defaults and connection settings](../reference/configuration.md).
