# Self-hosting

After the [local tutorial](../../examples/chat/README.md), use this guide to deploy with Modal and GCS.

Recommended setup:

- One always-on control plane near your database and hosts.
- Managed PostgreSQL with backups for deployments and public contracts.
- One GCS bucket for ownership, host leases, and snapshots.
- Modal hosts with matching runtime and SDK versions.

WebSocket connections live in control-plane memory: clients must reconnect after a restart. Multiple instances require gateway routing.

Use matching runtime-container and SDK versions that include `little-actors build`. The container includes the Rust runtime and Go provider; neither compiler is required. See [replication configuration](replication.md) for optional replica hosts and placement.

## 1. Configure storage and credentials

Create `control-plane.env` using [hosted server setup](../reference/configuration.md), then configure the [client connection](../reference/configuration.md) for your deployment terminal and backend. The configuration page contains all credentials, storage settings, and defaults used by this guide.

## 2. Run the control plane

Use the prebuilt runtime container:

```sh
docker run --rm --name durable-objects \
    -p 7100:7100 \
    --env-file control-plane.env \
    --mount type=bind,source=/absolute/path/to/service-account.json,target=/credentials/gcs.json,readonly \
    us-central1-docker.pkg.dev/fluid-analogy-473415-c2/public/little-actors:YOUR_VERSION
```

For an attached Google service account, follow the [Google credentials configuration](../reference/configuration.md) and omit the mount.

Port `7100` serves the HTTP API and WebSockets. Use an HTTPS proxy that forwards HTTP/2 and WebSockets. The public URL must be reachable by Modal hosts and clients.

From the configured deployment terminal, check the public endpoint:

```sh
curl --fail --silent --show-error https://objects.example.com/.well-known/jwks.json
```

Expect JSON with a `keys` array. Your first actor call will also exercise host provisioning and storage.

## 3. Package your actor code

In the chat project from the local tutorial, pin the SDK to the runtime version:

```sh
npm install --save-exact little-actors@YOUR_VERSION
```

Create a `Dockerfile` in your chat project:

```dockerfile
FROM us-central1-docker.pkg.dev/fluid-analogy-473415-c2/public/little-actors:YOUR_VERSION AS runtime

FROM node:22-bookworm
COPY --from=runtime /usr/local/bin/little-actors /usr/local/bin/little-actors
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY src ./src
COPY tsconfig.json ./
RUN npx little-actors build
```

The image combines the runtime, Node.js, and the compiled actor artifact. The build embeds schemas in `dist/actors.mjs`, so host startup does not run the TypeScript compiler.

Build and push an amd64 image to a registry you control:

```sh
docker buildx build --platform linux/amd64 \
    --tag YOUR_REGISTRY/chat-example:chat-v1 --push .
```

Configure [Modal image-build credentials](../reference/configuration.md), then import the image with Modal's Python API. This cloud-only step can run in CI:

```sh
python3 -m venv .venv
.venv/bin/python -m pip install modal
```

Create `build_image.py`:

```python
import modal

image = modal.Image.from_registry(
    "YOUR_REGISTRY/chat-example:chat-v1",
    add_python="3.12",
)
app = modal.App.lookup("chat-example-images", create_if_missing=True)
with modal.enable_output():
    image.build(app)
print(image.object_id)
```

Then run:

```sh
.venv/bin/python build_image.py
```

Keep the printed `im-...` ID. Private registries require a [Modal registry secret](https://modal.com/docs/guide/existing-images).

## 4. Register the deployment

From the actor project used to build the image, register it and publish its public API automatically. Use the image ID printed in step 3:

```sh
npx little-actors deploy \
    --image im-YOUR_IMAGE_ID \
    --working-directory /app \
    --actor-entrypoint dist/actors.mjs
```

## 5. Connect your web app

Start the application backend with the [client connection](../reference/configuration.md) configured in step 1.

Use the generated `actors.ChatRoom.prepareWebsocket()` helper as shown in the [browser chat demo](../../examples/chat/src/backend.ts), adding your application's authentication before issuing tickets. Generate the clients from the published API with `npx little-actors generate --url`, and have the frontend fetch a grant from that application route.

Start the web app with its normal tooling and open two signed-in browser sessions. A message in either session broadcasts the updated history to both sessions. Reloading a page supplies the current snapshot. Hosted state is separate from local demo state.

The proxy checks user access and obtains connection credentials. Application messages travel directly over WebSockets to the actor gateway. Keep the API key on the backend. See [gateway configuration](../reference/configuration.md) for a separate gateway origin.

## Local execution with GCS

Configure [local GCS storage](../reference/configuration.md), then start the runtime:

```sh
npx little-actors dev --storage gcs --data-dir .gcs-demo
```

Generate the [browser demo](../../examples/chat/README.md) SDK, set the same `DURABLE_OBJECT_API_KEY` on the runtime and application backend, and start your web app normally. Send a message and reload the page to see the saved conversation.

## Browser connections

For a separate gateway, see [client gateway configuration](../reference/configuration.md) and the [deployment request](../reference/http.md#put-v1deployment).

For browser connections, generate the backend helpers with `little-actors generate`. Expose an application endpoint that authenticates the user and checks access, then calls `actors.ChatRoom.prepareWebsocket({ actorId, metadata })` from the generated `index.ts`. Keep the API key on that backend. The helper obtains an actor-scoped ticket from the control plane, and the frontend calls `new WebSocket(grant.websocketUrl)` to connect directly to the gateway.

The returned URL contains its signed key. There is no browser SDK, custom handshake, state subscription, or automatic renewal. The gateway enforces expiration even while idle or running a handler. Your application handles closure and requests a new grant if it wants to reconnect.

See the [browser example](../../sdk/README.md#browser-clients) and [wire protocol](../reference/http.md#external-connections). The optional incoming-message event callback remains independent of authorization.
