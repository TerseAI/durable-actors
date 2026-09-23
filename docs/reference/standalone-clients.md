# Standalone generated clients

`durable-actors generate` produces a typed client that runs without installing `durable-actors` or any other npm package in the consuming application. Both RPC stubs and WebSocket authorization use the generated runtime. Actor projects still use the SDK to define and host actors.

## Application workflow

For a repeatable generation command, install the CLI as a development dependency:

```sh
pnpm add -D durable-actors
pnpm exec durable-actors generate
```

Alternatively, run `pnpm dlx durable-actors generate` from the application directory without changing its dependencies or lockfile. For CI, pin the CLI version with `durable-actors@VERSION`, using the version matching your actor deployment. The CLI needs access to the running control plane and its credentials during generation; the application needs its own environment settings at runtime.

Commit the entire generated directory:

```text
generated/
  index.ts
  runtime/
    index.ts / index.browser.ts
    stub.ts                  # Method wrappers and default transport
    client.ts                # Discovery, caching and safe retries
    http.ts                  # HTTP requests and response envelopes
    proxy.ts                 # WebSocket grants
    settings.ts              # Configuration and actor paths
    errors.ts / json.ts / telemetry.ts
    package.json / LICENSE.md
```

Import the client, configuration helper, and error class from that directory:

```ts
import { actors, createActorTransport, ActorInvocationError } from "./generated/index.js"

const counter = actors.Counter.get("example")
await counter.increment()

const transport = createActorTransport({
    projectId: "my-project",
    controlPlaneUrl: "https://actors.example.com",
    apiKey: process.env.DURABLE_ACTORS_SECRET
})
const remote = actors.Counter.get("example", transport)
try {
    await remote.increment()
} catch (error) {
    if (error instanceof ActorInvocationError) console.error(error.code, error.requestId)
    throw error
}
```

Use `ActorProxy.handle()` or `actors.Counter.prepareWebsocket()` to issue WebSocket grants. Their options and result types are exported by `generated/index.ts`.

Applications can compile the generated TypeScript using `tsc` or an ESM/CommonJS server bundler. The runtime uses standard `fetch`, `URL`, `crypto`, `performance` and `TextEncoder` APIs, with no npm imports or Node-specific modules. Browser bundlers select a small guard through `runtime/package.json`; obtain WebSocket grants from your backend, keeping the application credential there. Preserve that browser mapping if distributing compiled JavaScript, and include the license.

Calls use HTTP/JSON end to end: the control plane returns a host origin, short-lived ticket and ownership epoch; the client posts the method and JSON arguments directly to that host. The host delivers socket effects before returning the result. Browser connections use WebSockets.

The client refreshes an expired ticket or follows an explicit reroute once. Connection loss, HTTP server errors and actor failures are never automatically retried. A failed response with `outcome_unknown` means the method may have committed; retrying can execute it twice. See [the HTTP protocol](./openapi.yaml) for the request and response shapes.

## Application dependencies

Import application clients and their types from `./generated/index.js`. Build and deploy the generated directory with the application. The SDK is needed only for generation, so install it as a development dependency or use a pinned one-shot CLI command.

Development installs include the CLI's dependency tree. Production installs can omit development dependencies, and the one-shot CLI workflow keeps that tree out of the application lockfile entirely.

## Generation and release design

The SDK maintains dependency-free TypeScript in `sdk/src/client-runtime/`. Both the SDK's server client and the generator use that implementation. The build embeds those source files in the generator; generation copies them unchanged beside the contract-specific `index.ts`. There is no runtime bundling, minification, protobuf generation or third-party source vendoring.

`generateTypeScript()` returns all filenames and contents. Local source generation and generation from a published contract use the same process. Regeneration replaces the managed `runtime/` directory, so do not put application code there.

HTTP/JSON is the sole client-to-host protocol. Hosts and generated clients are released together; regenerate clients for each deployment version. The generator supports the current output layout only. Internal control-plane and storage services use their own gRPC protocols.

Release checks compile a generated client with no dependencies or Node type packages, run it both unbundled and in ESM/CommonJS bundles, and exercise calls, errors and WebSocket grants against local HTTP servers. Rust integration tests verify the real host, capability checks and socket effects. Browser bundling is checked independently.
