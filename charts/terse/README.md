# Terse on GKE Agent Substrate

The chart runs the Rust control plane and HTTPS/WebSocket gateway in Kubernetes. Substrate packs isolated gVisor actor sandboxes into shared WorkerPool Pods. State writes go directly to precreated GCS Rapid buckets; no storage pods are started, assigned, or contacted during activation.

Each write appends the same state record to persistent streams in two Rapid zones and waits for both durable flushes. Standard GCS stores manifests, checkpoints, and archived segments. The host opens connections alongside code and state loading; read-only activation does not wait for log setup.

## Prerequisites

- GKE Standard with Workload Identity, Gateway API, and Agent Substrate installed. This chart targets Substrate `v0.2.0-gke.0`. Enable its certificate APIs before creating nodes; existing nodes may not support ClusterTrustBundle projection. Run control-plane Pods on ordinary nodes and workers on Substrate-compatible nodes.
- Standard authority, artifact, and archive buckets with uniform access and public access prevention. Keep the archive indefinitely; its location determines the permanent failure domain. Do not expire referenced actor history or code.
- Two Rapid buckets in supported distinct zones, without automatic deletion of log objects. Buckets are shared infrastructure; actor data is isolated by credential prefixes.
- PostgreSQL, preferably private Cloud SQL with regional HA. The database user needs migration privileges.
- A Google service account with `roles/storage.objectUser` on the application buckets and `storage.buckets.get` on the Rapid and archive buckets. `roles/storage.legacyBucketReader` supplies the latter at bucket scope. Bind `<control-namespace>/<release>-terse` with `roles/iam.workloadIdentityUser`; customer sandboxes use downscoped credentials and cannot reach the metadata server.
- A control-namespace Secret containing `postgres-url`, `api-key`, and `jwt-signing-key` (base64 Ed25519 PKCS#8).
- A matching TLS Secret or Google-managed Compute Engine certificate. Set exactly one of `gateway.tlsSecret` and `gateway.preSharedCert`.

## Provision storage

Use Google Cloud CLI 553 or newer. Substitute your own globally unique bucket names. Create each Rapid bucket once:

```sh
gcloud storage buckets create gs://ACTOR_RAPID_A --project=PROJECT \
  --location=us-west4 --placement=us-west4-a --default-storage-class=RAPID \
  --enable-hierarchical-namespace --uniform-bucket-level-access --public-access-prevention
gcloud storage buckets create gs://ACTOR_RAPID_B --project=PROJECT \
  --location=us-west4 --placement=us-west4-b --default-storage-class=RAPID \
  --enable-hierarchical-namespace --uniform-bucket-level-access --public-access-prevention
```

The application archives segments before deleting their Rapid copies. Do not configure lifecycle deletion for `durable-actors-v3-logs-`, or for permanent Standard state. Startup rejects Rapid deletion rules that could match the log prefix. A crashed host's retained logs must remain available until safely archived.

## Install

Copy `values.yaml` and supply the image digest, Google service account, bucket names, namespaces, and region:

```yaml
replicaCount: 2
region: north-america-west
substrate:
  atespace: terse-staging
  snapshotLocation: gs://actor-snapshots/runtime/
  worker:
    replicas: 1
    cpu: "4"
    memory: 8Gi
cloudSql:
  instanceConnectionName: project:us-west4:actors-db
storage:
  authorityBucket: actor-ownership
  artifactBucket: actor-code
  archiveBucket: actor-archive
  rapid:
    buckets:
      - {bucket: actor-rapid-a, zone: us-west4-a}
      - {bucket: actor-rapid-b, zone: us-west4-b}
```

Exactly two Rapid buckets in different zones are required. Startup verifies their actual GCS storage class, placement, and retention rules. Keep storage configuration identical across controllers and immutable for existing ownership records.

```sh
helm lint charts/terse -f production-values.yaml
helm template actors charts/terse --namespace terse-control -f production-values.yaml
helm upgrade --install actors charts/terse --namespace terse-control --create-namespace -f production-values.yaml
```

With Cloud SQL, `postgres-url` points to `127.0.0.1:5432`; the native proxy sidecar starts before the control plane. Leave `cloudSql.instanceConnectionName` empty for a direct database connection. Configure DNS and certificates separately.

## Writes and recovery

The first write waits for a durable manifest binding both object generations to the actor's ownership epoch. Subsequent writes reuse those streams; they perform no finalize, rename, ownership update, or Standard upload. A failed or cancelled write fences that activation. If either Rapid stream cannot be opened, the activation uses immutable Standard snapshots.

Segments rotate at 8 MiB, with a checkpoint worker checking every 60 seconds. Clean shutdown archives all records and records the final snapshot in ownership. Clean resume reads that snapshot directly. Crash recovery waits for lease expiry, fences available old streams, verifies records, and makes recovered state durable in Standard before claiming a new ownership epoch. One surviving Rapid zone can recover acknowledged records because every acknowledgment required both copies. An interrupted request can have an uncertain outcome.

Actor-scoped credentials cover only the ownership object, the actor's log and archive prefixes, and its immutable deployment code.

## Capacity and deployment

### Shared workers and prepared snapshots

The chart creates one `WorkerPool`. Its CPU and memory are shared capacity, not an actor profile. Each actor's `@Sandbox` CPU and memory become Substrate resource limits. A 4-CPU worker can therefore host different sizes concurrently, provided their combined reservations fit. Increase worker replicas or worker size for additional capacity; allow room for preparing golden snapshots too. Node autoscaling must also have quota and room for those Pods.

Deployment registration prepares an immutable `ActorTemplate` and its golden snapshot for every declared resource shape in the control plane's configured region. Template identity includes the pinned image, resource limits, verification keys, region, and snapshot configuration. Preparation boots the generic Rust/Bun/Litestream runtime and waits for `/warmz`. With `substrate.codeSnapshots: true` (the default), the control plane then clones a preparation sandbox, streams generation-pinned customer code from GCS to `/prepare-code`, and captures a separate snapshot tag before publishing the deployment. Tags are keyed by template UID and artifact manifest, so unchanged code reuses its prepared snapshot. Customer modules do not execute during preparation.

Activation clones the prepared code snapshot, applies the actor's egress policy, resumes the sandbox, and calls `/assign`. Assignment requires a short-lived signed capability bound to the current Substrate actor UID. The SystemInfo volume supplies that UID; verification rereads it after cloning. Prepared snapshots contain code bytes but no customer credentials, actor ownership, or durable state. After assignment, the host verifies the code hash locally and restores durable state through the existing GCS protocol. Set `substrate.codeSnapshots: false` to clone only the generic golden snapshot and download code after assignment; this provides a comparison baseline. Idle hosts shut down and release capacity. The provider probes `/warmz` every two seconds and deletes a sandbox after three failed probes, using UID/version preconditions. This releases reservations even when Substrate still reports an exited process as running. Probes do not reset actor idle time.

Snapshot preparation moves first-time initialization into deployment. A fresh worker node still needs to prepare the OCI image in Substrate's own cache. Kubernetes image pre-pulling alone does not populate that cache. Measure fresh-node startup separately from restoration on a prepared worker.

The chart projects a rotating Kubernetes service-account token and Substrate CA bundle into the control plane. Only the control plane accesses the private Substrate API. Customer sandboxes receive actor-scoped storage credentials and public verification keys. The default egress rules allow public IPv4 plus the private control-plane Service IP, excluding metadata and private network ranges. Customer Kubernetes secrets must have label `terse.ai/customer-secret=true`.

The snapshot bucket is separate from customer storage. Grant the Substrate `ate-api-server` and `atelet` service-account principals bucket-scoped object access and bucket metadata access. Follow the pinned Substrate installation instructions for API authentication, certificates, and worker networking.

```sh
kubectl --kubeconfig "$TERSE_STAGING_KUBECONFIG" get workerpools,pods -n terse-substrate
```

The Rust API client and protocol bindings are maintained in [terse-substrate](https://github.com/TerseAI/terse-substrate), pinned by commit in `Cargo.toml`. This backend replaces the old spare registry and per-profile warm pools. The pre-launch initial schema contains only current runtime tables; initialize a fresh PostgreSQL database for this backend. Actor ownership and durable state remain in the GCS storage protocol.

### WebSocket capacity

WebSockets can use independently sized replicas of the same control-plane image:

```yaml
sockets:
  dedicatedGateway: true
  replicaCount: 4
  maxConnectionsPerActor: 32768
  resources:
    requests: {cpu: "1", memory: 1Gi}
    limits: {memory: 2Gi}
```

This adds a `-sockets` Deployment, Service, disruption budget, and GKE backend/health policies. The HTTPS route sends exact `/v1/socket` requests to those replicas; other routes and internal host RPCs use the original control-plane Service. `sockets.nodeSelector` controls placement separately. Isolation is disabled by default. With an external ingress (`gateway.enabled: false`), configure the equivalent WebSocket route yourself.

The connection limit applies per actor at its socket-owning gateway, even with isolation disabled. PostgreSQL coordinates room ownership across replicas; ordinary control-plane pods are ineligible when dedicated gateways are enabled. See the [runtime limits](../../docs/reference/configuration.md#websockets). Scale socket replicas for aggregate connections and traffic; a busy individual actor still executes on one host. Changing the route or replacing gateway pods disconnects their existing sockets. Idle sandbox shutdown preserves connections, metadata, tags, and automatic responses at the gateway. The next application message activates a replacement sandbox.

The chart defaults to two control-plane replicas and one 4-CPU, 8-GiB Substrate worker. These are staging-oriented capacity defaults, not a production availability guarantee. Increase and distribute workers across failure domains before production use. Keep the runtime image consistent across control-plane replicas during deployment; old actor hosts retire through the normal ownership protocol.
