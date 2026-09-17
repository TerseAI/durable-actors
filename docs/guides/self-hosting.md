# Self-hosting

After the [local tutorial](../../examples/chat/README.md), use this guide to deploy with Modal and GCS.

Recommended setup:

- One always-on control plane near your database and hosts.
- Managed PostgreSQL with backups.
- A STANDARD GCS bucket near each actor region.
- Modal hosts with matching runtime and SDK versions.

WebSocket connections live in control-plane memory: clients must reconnect after a restart. Multiple instances require gateway routing.

The commands below use `YOUR_VERSION` for matching SDK and runtime builds from this source tree. Published older versions do not support the actor build artifact or bucket authority described here.

## 1. Configure storage and credentials

Create `control-plane.env`:

```dotenv
DURABLE_OBJECT_PROCESS_ROLE=control_plane
DURABLE_OBJECT_CONTROL_PLANE_BIND=0.0.0.0:7100
DURABLE_OBJECT_CONTROL_PLANE_URL=https://objects.example.com
DURABLE_OBJECT_POSTGRES_URL=postgresql://USER:PASSWORD@DB_HOST/durable_objects?sslmode=require
DURABLE_OBJECT_COORDINATION_BUCKET=my-actor-state-bucket
DURABLE_OBJECT_STANDARD_BUCKETS={"north-america-east":"my-actor-state-bucket"}
GOOGLE_APPLICATION_CREDENTIALS=/credentials/gcs.json
DURABLE_OBJECT_SANDBOX_PROVIDER=modal
MODAL_TOKEN_ID=YOUR_MODAL_TOKEN_ID
MODAL_TOKEN_SECRET=YOUR_MODAL_TOKEN_SECRET
DURABLE_OBJECT_JWT_SIGNING_KEY=YOUR_BASE64_PKCS8_KEY
DURABLE_OBJECT_API_KEY=YOUR_ADMIN_API_KEY
```

Replace the placeholders and keep this file out of source control.

PostgreSQL must be reachable for administrative changes; the hosted runtime can start and serve existing deployments while PostgreSQL is unavailable. Use your provider's TLS settings; the database user needs permission to run automatic migrations. `localhost` refers to the container.

`north-america-east` maps to Modal's `us-east`. Enter a nearby bucket's name without `gs://`. GCS holds snapshots, ownership, host leases, and published runtime configuration. PostgreSQL retains administrative deployment records. Back up the database and retain runtime bucket metadata and referenced snapshots.

The Google service account needs object read, list, create, and replace permissions. Hosts use scoped capabilities through the control plane and replica hosts. On Google Cloud, an attached service account can replace the key file. See [bucket authority and replica snapshots](replication.md) for lease clock requirements and recovery behavior.

Use Modal credentials from the workspace that owns your actor image.

Generate the signing key and admin API key:

```sh
openssl genpkey -algorithm Ed25519 -outform DER | base64 | tr -d '\n'
openssl rand -hex 32
```

Run each command separately and copy its output into the corresponding field. Reuse both keys after restarts. The runtime uses the signing key internally. Your backend uses the API key to deploy and call actors.

## 2. Run the control plane

Use the prebuilt runtime container:

```sh
docker run --rm --name durable-objects \
    -p 7100:7100 \
    --env-file control-plane.env \
    --mount type=bind,source=/absolute/path/to/service-account.json,target=/credentials/gcs.json,readonly \
    us-central1-docker.pkg.dev/fluid-analogy-473415-c2/public/little-actors:YOUR_VERSION
```

For an attached Google service account, omit the credential variable and mount.

Port `7100` serves the HTTP API and WebSockets. Use an HTTPS proxy that forwards HTTP/2 and WebSockets. The public URL must be reachable by Modal hosts and clients.

From another terminal, check the public endpoint:

```sh
export DURABLE_OBJECT_CONTROL_PLANE_URL='https://objects.example.com'
curl --fail --silent --show-error "$DURABLE_OBJECT_CONTROL_PLANE_URL/.well-known/jwks.json"
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

FROM node:22-bookworm AS actors
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY tsconfig.json ./
COPY src ./src
RUN npx little-actors build

FROM node:22-bookworm
COPY --from=runtime /usr/local/bin/little-actors /usr/local/bin/little-actors
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY --from=actors /app/dist/actors.mjs ./dist/actors.mjs
```

The image combines the runtime, Node.js, production dependencies, and the built actor artifact. Actor checks and schema generation happen during the image build. Startup imports the artifact inside an actor Worker without running the TypeScript compiler or `tsx`.

Build and push an amd64 image to a registry you control:

```sh
docker buildx build --platform linux/amd64 \
    --tag YOUR_REGISTRY/chat-example:chat-v1 --push .
```

Import the image with Modal's Python API. This cloud-only step can run in CI:

```sh
python3 -m venv .venv
.venv/bin/python -m pip install modal
export MODAL_TOKEN_ID='<your-modal-token-id>'
export MODAL_TOKEN_SECRET='<your-modal-token-secret>'
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

Register the image and actor file:

```sh
export DURABLE_OBJECT_API_KEY='<the-admin-api-key-from-step-1>'
export ACTOR_IMAGE_ID='<the-im-prefixed-image-id-from-step-3>'

curl --fail --silent --show-error \
    -X PUT "$DURABLE_OBJECT_CONTROL_PLANE_URL/v1/deployment" \
    -H "Authorization: Bearer $DURABLE_OBJECT_API_KEY" \
    -H 'Content-Type: application/json' \
    --data @- <<EOF
{
    "codeRevision": "chat-v1",
    "imageRef": "$ACTOR_IMAGE_ID",
    "workingDirectory": "/app",
    "actorEntrypoint": "dist/actors.mjs"
}
EOF
```

Registration returns `{"changed":true}`, or `false` for an unchanged deployment. The first call starts a host. After code changes, rebuild and import the image, then register its ID with a new `codeRevision`.

## 5. Connect your web app

Set the API key and server URL in your application proxy's backend environment:

```sh
export DURABLE_OBJECT_API_KEY='<the-api-key-from-step-1>'
export DURABLE_OBJECT_CONTROL_PLANE_URL='https://objects.example.com'
```

Use the generated `ActorProxy` as shown in the [browser chat demo](../../examples/chat/src/backend.ts), adding your application's authentication before issuing tickets. Generate the client and proxy from the deployed actor source with `little-actors generate`, and point the frontend client at that application route.

Start the web app with its normal tooling and open two signed-in browser sessions. A message in either session updates both histories after persistence. Reloading a page supplies the current snapshot. Hosted state is separate from local demo state.

The proxy checks user access and obtains connection credentials. Application messages travel directly over WebSockets to the actor gateway. Keep the API key on the backend. See [WebSocket configuration](#websocket-configuration) for a separate gateway origin.

## Local execution with GCS

To save snapshots in GCS while running actors locally:

```sh
export DURABLE_OBJECT_STANDARD_BUCKETS='{"north-america-east":"my-actor-state-bucket"}'
export GOOGLE_APPLICATION_CREDENTIALS='/absolute/path/to/service-account.json'

npx little-actors dev --storage gcs --data-dir .gcs-demo
```

Generate the [browser demo](../../examples/chat/README.md) SDK, point its proxy at the local server using `.gcs-demo/runtime.json`, and start your web app normally. Send a message and reload the page to see the saved conversation.

Changing backends or buckets requires a separate state directory; existing actors are not migrated. Local development references remain in SQLite, so losing that file loses access to those local actors. The hosted runtime uses bucket authority instead.

## WebSocket configuration

WebSockets use the control-plane origin by default. For a separate gateway, set `DURABLE_OBJECT_SOCKET_GATEWAY_URL` for clients and `socketGatewayUrl` in the deployment.

For browser clients, generate the typed client and proxy with `little-actors generate`. Expose an application endpoint that authenticates the user and checks access, then calls `ActorProxy.handle()` from the generated `proxy.ts`. Keep `DURABLE_OBJECT_API_KEY` on that backend. The helper obtains an actor-scoped ticket from the control plane, and the browser SDK connects directly to the gateway.

Connection and renewal use the same application endpoint. The SDK renews authorization over the existing socket; unchanged authorized metadata preserves actor-modified metadata and tags. Changed metadata reconnects through `onConnect`. The gateway enforces expiration even while idle or running a handler.

See the [browser example](../../sdk/README.md#browser-clients) and [wire protocol](../reference/http.md#external-connections). The optional incoming-message event callback remains independent of authorization.

## Server configuration

These environment variables configure the hosted server, including [`start`](../reference/cli.md#start-a-hosted-server). Required values must be nonempty. SDK client configuration is documented [separately](../reference/api.md#client-configuration).

### `DURABLE_OBJECT_POSTGRES_URL`

**Required.**

PostgreSQL connection URL for administrative deployment data.

### `DURABLE_OBJECT_COORDINATION_BUCKET`

**Required for hosted servers.** Bucket containing actor ownership, host leases, and published runtime configuration. It may also be a snapshot bucket. All control-plane instances must use the same value.

### `DURABLE_OBJECT_STANDARD_BUCKETS`

**Required.**

Nonempty JSON region-to-bucket map. Region names contain 1–64 lowercase ASCII letters, digits, `.`, `_`, or `-`.

### `DURABLE_OBJECT_API_KEY`

**Required.**

Backend bearer credential for deployments and actor access; no surrounding whitespace. Also authenticates outgoing WebSocket callbacks.

### `DURABLE_OBJECT_JWT_SIGNING_KEY`

**Required.**

Base64-encoded Ed25519 private key in PKCS#8 format.

### `DURABLE_OBJECT_SANDBOX_PROVIDER`

**Required:** `modal`.

Cloud execution provider.

### `MODAL_TOKEN_ID`

**Required.**

Modal token ID, without surrounding whitespace.

### `MODAL_TOKEN_SECRET`

**Required.**

Modal token secret, without surrounding whitespace.

### `DURABLE_OBJECT_CONTROL_PLANE_URL`

**Required.**

Reachable HTTP(S) server origin.

### `DURABLE_OBJECT_CONTROL_PLANE_BIND`

**Default:** `127.0.0.1:7100`.

Listening IP address and port.

### `DURABLE_OBJECT_JWT_KEY_ID`

**Default:** `primary`.

Signing key identifier.

### `DURABLE_OBJECT_JWT_ISSUER`

**Default:** `durable-object-control-plane`.

Token issuer.

### `DURABLE_OBJECT_AUTHORITY_JWT_AUDIENCE`

**Default:** `durable-object-authority`.

Audience for server authentication.

### `DURABLE_OBJECT_INVOKE_JWT_AUDIENCE`

**Default:** `durable-object-invoke`.

Audience for actor calls.

### `DURABLE_OBJECT_JWT_MAX_TTL_SECONDS`

**Default:** `86400`.

Positive maximum token lifetime; session tokens are additionally capped at 24 hours.

### `DURABLE_OBJECT_ACTOR_IDLE_TIMEOUT_MS`

**Default:** `60000`.

Idle time before an actor may hibernate. Valid range: 1–86400000.

### `DURABLE_OBJECT_HOST_IDLE_TIMEOUT_MS`

**Default:** `300000`.

Idle time before an unused cloud host may stop. Valid range: 1–86400000.

### `DURABLE_OBJECT_SANDBOX_COMMAND`

**Default:** Bundled provider executable.

Override the cloud provider executable when supplying a custom runtime distribution.

### `DURABLE_OBJECT_SOCKET_EVENT_URL`

**Default:** Disabled.

HTTP(S) callback receiving successfully handled incoming WebSocket messages.

### `GOOGLE_APPLICATION_CREDENTIALS`

**Default:** Google Application Default Credentials discovery.

Path to a service-account credentials file for GCS. An attached Google identity can also supply credentials. See [storage and credentials](#1-configure-storage-and-credentials) for access requirements.

### `RUST_LOG`

**Default:** `info`.

Runtime log filter, for example `warn` or `debug`.

## Further configuration

See [advanced access configuration](advanced-access.md) for integrations that need separate scopes or delegated credentials.
