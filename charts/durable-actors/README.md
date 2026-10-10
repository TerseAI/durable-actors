# Terse on GKE Agent Substrate

The chart runs the Rust control plane and HTTPS/WebSocket gateway in Kubernetes. Substrate packs isolated gVisor actor sandboxes into shared WorkerPool Pods. State writes go directly to precreated GCS Rapid buckets; no storage pods are started, assigned, or contacted during activation.

Each write appends the same state record to persistent streams in two Rapid zones and waits for both durable flushes. Standard GCS stores manifests, checkpoints, and archived segments. The host opens connections alongside code and state loading; read-only activation does not wait for log setup.

## Prerequisites

- GKE Standard with Workload Identity, Gateway API, and Agent Substrate installed. This chart targets Substrate `v0.2.0-gke.0`. Enable its certificate APIs before creating nodes; existing nodes may not support ClusterTrustBundle projection. Run the Terse and Substrate control services (including the Substrate database and certificate controller) on the control node pool. Keep workers and node agents on the autoscaled Substrate node pool; colocated control services can block node removal through their disruption budgets.
- GKE 1.36.2-gke.2771000 or newer with Managed Service for Prometheus and the managed autoscaling metrics adapter. Enable node autoscaling on the Substrate node pool with a maximum large enough to schedule `substrate.worker.autoscaling.maxReplicas` worker Pods.
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

Copy `values.yaml` and supply all three image digests (`images.controlPlane`, `images.typescript`, and `images.python`), Google service account, bucket names, namespaces, and region:

```yaml
cluster: {projectId: PROJECT, location: us-west4, name: actors}
replicaCount: 2
region: north-america-west
substrate:
  atespace: terse
  snapshotLocation: gs://actor-snapshots/runtime/
  worker:
    autoscaling:
      minReplicas: 1
      maxReplicas: 3
      targetAllocationPercent: 70
      scaleDownStabilizationSeconds: 300
    cpu: "4"
    memory: 8Gi
cloudSql:
  instanceConnectionName: project:us-west4:actors-db
storage:
  mode: rapid
  authorityBucket: actor-ownership
  artifactBucket: actor-code
  archiveBucket: actor-archive
  rapid:
    buckets:
      - {bucket: actor-rapid-a, zone: us-west4-a}
      - {bucket: actor-rapid-b, zone: us-west4-b}
```

For `storage.mode: rapid`, exactly two Rapid buckets in different zones are required. To use Standard GCS only, set `storage.mode: standard` and `storage.rapid.buckets: []`; the authority and artifact buckets may be the same bucket. Startup verifies their actual GCS storage class, placement, and retention rules. Keep storage configuration identical across controllers and immutable for existing ownership records.

```sh
helm lint charts/durable-actors -f production-values.yaml
helm template actors charts/durable-actors --namespace terse-control -f production-values.yaml
helm upgrade --install actors charts/durable-actors --namespace terse-control --create-namespace -f production-values.yaml
```

With Cloud SQL, `postgres-url` points to `127.0.0.1:5432`; the native proxy sidecar starts before the control plane. Leave `cloudSql.instanceConnectionName` empty for a direct database connection. Configure DNS and certificates separately.

## Writes and recovery

The first write waits for a durable manifest binding both object generations to the actor's ownership epoch. Subsequent writes reuse those streams; they perform no finalize, rename, ownership update, or Standard upload. A failed or cancelled write fences that activation. If either Rapid stream cannot be opened, the activation uses immutable Standard snapshots.

Segments rotate at 8 MiB, with a checkpoint worker checking every 60 seconds. Clean shutdown archives all records and records the final snapshot in ownership. Clean resume reads that snapshot directly. Crash recovery waits for lease expiry, fences available old streams, verifies records, and makes recovered state durable in Standard before claiming a new ownership epoch. One surviving Rapid zone can recover acknowledged records because every acknowledgment required both copies. An interrupted request can have an uncertain outcome.

Actor-scoped credentials cover only the ownership object, the actor's log and archive prefixes, and its immutable deployment code.

## Capacity and deployment

The chart creates a shared `WorkerPool`. Each actor's `@Compute` CPU and memory become Substrate resource limits. Size `substrate.worker` for concurrent actor reservations and snapshot preparation; worker replicas and nodes must have enough capacity for those reservations.

Deployment registration prepares snapshots containing the runtime and customer code for each resource shape. Customer modules execute only after activation. Snapshot restoration runs alongside scoped credential issuance and ownership acquisition; `/assign` passes the ownership grant to the host, which verifies the restored code and remaining lease before becoming ready. Idle hosts shut down and release worker capacity.

The chart projects the Substrate API token and CA bundle into the control plane. Customer secrets require the label `terse.ai/customer-secret=true`. The separate snapshot bucket requires object access and bucket metadata access for the Substrate `ate-api-server` and `atelet` service-account principals. Follow the pinned Substrate installation instructions for API authentication and worker networking.

Migration V18 removes the retired spare tables; V1–V17 and existing actor state are preserved.

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

The chart defaults to two control-plane replicas and 1–3 workers of 4 CPU and 8 GiB each. HPA scales the WorkerPool using each worker’s largest reserved CPU, memory, or actor-slot fraction, targeting 70% allocation. The control plane exposes this metric on internal port 9091; GKE collects it and deduplicates the control-plane replicas. Missing or stale capacity data prevents metric-driven scaling. Scale-down waits five minutes and removes at most one worker per minute through Substrate’s graceful shutdown path. GKE node autoscaling adds machines when worker Pods cannot be scheduled and removes unneeded machines later.

Set the worker minimum for expected bursts and distribute workers across failure domains. New nodes take time to start; this headroom does not queue activations when the pool is exhausted. An actor must fit within a single worker. Keep the runtime image consistent across control-plane replicas.
