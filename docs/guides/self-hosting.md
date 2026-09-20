# Self-hosting

After the [local tutorial](../../examples/chat/README.md), use this guide to deploy with Modal and GCS.

Recommended setup:

- One always-on control plane near your database and hosts.
- Managed PostgreSQL with backups for deployments, public contracts, and exclusive sandbox claims.
- One GCS bucket for combined ownership and activation leases, replication sessions, and snapshots.
- Modal hosts with matching runtime and SDK versions.

Use matching runtime-container and SDK versions that include `little-actors build`. The generic container includes the Rust runtime, Bun, SDK, and Go provider; neither compiler is required. See [replication configuration](replication.md) for optional replica hosts and placement.

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

Port `7100` serves the HTTP and gRPC control-plane APIs. Use an HTTPS proxy that forwards HTTP/2. The public URL must be reachable by Modal hosts and clients. Direct WebSockets use the actor host on port `7101` through Modal ingress.

From the configured deployment terminal, check the public endpoint:

```sh
curl --fail --silent --show-error https://objects.example.com/.well-known/jwks.json
```

Expect JSON with a `keys` array. Your first actor call will also exercise host provisioning and storage.

## 3. Publish the generic runtime image

Build and publish this repository's runtime image once per runtime version. It contains Rust, Bun, and the matching SDK. Import that image into Modal; customer code is published separately. Use the same SDK version in the actor source project:

```sh
npm install --save-exact little-actors@YOUR_VERSION
python3 -m venv .venv
.venv/bin/python -m pip install modal
```

Create `build_image.py`:

```python
import modal

image = modal.Image.from_registry(
    "us-central1-docker.pkg.dev/fluid-analogy-473415-c2/public/little-actors:YOUR_VERSION",
    add_python="3.12",
)
app = modal.App.lookup("little-actors-runtime-images", create_if_missing=True)
with modal.enable_output():
    image.build(app)
print(image.object_id)
```

Run `.venv/bin/python build_image.py` and keep the printed `im-...` ID. Private registries require a Modal registry secret. For an unreleased checkout, build and push its Dockerfile to your own registry and import that tag.

## 4. Publish customer code and register the deployment

Configure Modal credentials in the deployment terminal, then run from the actor source project:

```sh
npx little-actors deploy --image im-YOUR_RUNTIME_IMAGE_ID
```

The CLI checks the actor contract, bundles customer code and JavaScript dependencies into `actors.mjs`, and publishes a permanent Modal directory snapshot. Only after publication succeeds does it register the snapshot, generic image, and contract. The shared SDK remains external to the bundle. Native add-ons and additional filesystem assets require separate packaging support.

Spare sandboxes initialize Rust, Bun, the SDK worker, and IPC before admission. On assignment, the provider mounts the immutable code snapshot while Rust restores committed state. Routing begins after Bun has loaded the code and hydrated the actor. A sandbox belongs to that actor for its entire lifetime; subsequent calls reuse it. A crash or sandbox expiration starts a fresh activation from committed state, using the existing bucket and replication machinery.

Configure spare capacity, regions, lifetimes, and resource limits using the [configuration table](../reference/configuration.md).

Each actor activation claims dedicated Rust-only replica listeners in parallel with its primary. These spares start without an actor identity or state, then accept an authenticated assignment. Initial writes confirm through GCS while replicas initialize and catch up independently. The primary enables replica acknowledgments after a conditional membership change and a local state-version check. Failed replicas are replaced through the same pool, while writes continue through GCS. Catch-up and cleanup preserve recovery witnesses; see [replica lifecycle](replication.md#repair-and-lifecycle). Hosts reuse gRPC connections for replica initialization, recovery, and writes, with credentials supplied per request.

Old code snapshots remain immutable deployment artifacts; retiring hosts does not delete snapshots. Track published snapshot IDs if you need artifact retention cleanup.

## 5. Connect your web app

Start the application backend with the [client connection](../reference/configuration.md) configured in step 1.

Use the generated `actors.ChatRoom.prepareWebsocket()` helper as shown in the [browser chat demo](../../examples/chat/src/backend.ts), adding your application's authentication before issuing tickets. Generate the clients from the published API with `npx little-actors generate --url`, and have the frontend fetch a grant from that application route.

Start the web app with its normal tooling and open two signed-in browser sessions. A message in either session broadcasts the updated history to both sessions. Reloading a page supplies the current snapshot. Hosted state is separate from local demo state.

The application backend checks user access and obtains connection credentials. Application messages travel directly over WebSockets to the actor host. Keep the API key on the backend and preserve the returned URL, including its Modal token and actor ticket.

## Local execution with GCS

Configure [local GCS storage](../reference/configuration.md), then start the runtime:

```sh
npx little-actors dev --storage gcs --data-dir .gcs-demo
```

Generate the [browser demo](../../examples/chat/README.md) SDK, set the same `DURABLE_OBJECT_API_KEY` on the runtime and application backend, and start your web app normally. Send a message and reload the page to see the saved conversation.

## Browser connections

See the [browser example](../../sdk/README.md#browser-clients) and [wire protocol](../reference/http.md#external-connections). The optional incoming-message event callback remains independent of authorization.

## Regional installations

Regional control planes share one active code deployment in PostgreSQL, one state bucket, and the same signing key and API key. Set `DURABLE_OBJECT_REGION` on each regional instance. The production deployment repository owns the load balancer, persistent actor-home directory, geographic selection, and forwarding setup requests with an assigned `homeRegion`.

Each `(actor type, actor ID)` has its own state and permanent home. Customer proxies authenticate users and enforce access before requesting actor capabilities. WebSockets connect directly to the owning Modal host using the returned URL. Independent installations require separate databases, buckets, credentials, and Modal resources.
