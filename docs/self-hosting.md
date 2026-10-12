# Self-host Durable Actors on GCP

Deploy Durable Actors on GKE Agent Substrate with PostgreSQL and one Standard GCS bucket. Standard GCS is the default. GCS Rapid is an optional acceleration for workloads that need lower state-write latency.

## Prerequisites

- GKE Standard with Workload Identity Federation and enforced NetworkPolicy. Install Agent Substrate and use a separate worker node pool. The chart targets Substrate `v0.2.0-gke.0`; follow the [Substrate prerequisites](../charts/durable-actors/README.md#prerequisites), including projected certificates and managed autoscaling metrics.
- PostgreSQL reachable from the controller, with a database user allowed to run migrations. Use a TLS connection for a remote database. A private Cloud SQL instance is suitable when your cluster has a supported connection to it.
- An HTTPS hostname and certificate. The chart can create a GKE Gateway, or you can route an existing HTTPS gateway to its Service. The endpoint must support WebSockets.
- Helm, `kubectl`, Google Cloud CLI, and permissions to create the following resources.

Google Cloud is the supported hosting platform. [GKE Agent Substrate setup](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/sandbox-pods) and [Workload Identity setup](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/workload-identity) describe the cluster configuration.

## Create the bucket and identity

Choose a globally unique bucket name and a region near your actor nodes. These examples use `us-west4` and release/namespace `actors`.

```sh
export PROJECT=my-google-project
export BUCKET=my-unique-actor-state
export GSA="actors@${PROJECT}.iam.gserviceaccount.com"

gcloud storage buckets create "gs://${BUCKET}" --project="$PROJECT" \
  --location=us-west4 --default-storage-class=STANDARD \
  --uniform-bucket-level-access --public-access-prevention
gcloud iam service-accounts create actors --project="$PROJECT"
gcloud storage buckets add-iam-policy-binding "gs://${BUCKET}" \
  --member="serviceAccount:${GSA}" --role=roles/storage.objectUser
gcloud iam service-accounts add-iam-policy-binding "$GSA" --project="$PROJECT" \
  --role=roles/iam.workloadIdentityUser \
  --member="serviceAccount:${PROJECT}.svc.id.goog[actors/actors-terse]"
```

The bucket stores ownership, immutable code, and state snapshots under separate prefixes. Actor sandboxes receive credentials scoped to their own state and deployed code. Do not configure lifecycle deletion for referenced state or code. Take PostgreSQL backups and rehearse recovery alongside the retained bucket data.

## Create credentials

Create the controller namespace and a stable signing key. Set `POSTGRES_URL` to your reachable PostgreSQL connection string. The Secret remains stable across chart upgrades.

```sh
kubectl create namespace actors
umask 077
openssl genpkey -algorithm ED25519 -outform DER | openssl base64 -A > jwt-signing-key
openssl rand -hex 32 | tr -d '\n' > api-key
kubectl -n actors create secret generic actors-credentials \
  --from-literal=postgres-url="$POSTGRES_URL" \
  --from-file=jwt-signing-key --from-file=api-key
```

Store the generated credentials in your secret manager. Configure your backend with the same API key. Provision your TLS Secret separately in namespace `actors`; its certificate must cover your public hostname.

## Install

Create `actors-values.yaml`, substituting your actual bucket, identity, hostname, zone, ingress class, and TLS Secret:

```yaml
publicUrl: https://actors.example.com
cluster: {projectId: my-google-project, location: us-west4-a, name: actors}
region: north-america-west
credentialsSecret: actors-credentials
substrate:
  snapshotLocation: gs://my-actor-snapshots/runtime/
storage:
  mode: standard
  authorityBucket: my-unique-actor-state
  artifactBucket: my-unique-actor-state
  rapid:
    buckets: []
serviceAccount:
  googleServiceAccount: actors@my-google-project.iam.gserviceaccount.com
gateway:
  enabled: true
  tlsSecret: actors-tls
```

Set `VERSION` to the desired [Durable Actors release](https://github.com/TerseAI/durable-actors/releases). Published charts already pin the corresponding control-plane, TypeScript, and Python image digests.

```sh
helm upgrade --install actors \
  "https://github.com/TerseAI/durable-actors/releases/download/v${VERSION}/durable-actors-${VERSION}.tgz" \
  --namespace actors --values actors-values.yaml --wait --timeout 10m
kubectl -n actors rollout status deployment/actors-terse
```

The same chart is published at `oci://ghcr.io/terseai/charts/durable-actors`; use that reference with `--version "$VERSION"` if you prefer OCI distribution.

Provision a private snapshot bucket and grant object and bucket metadata access to the Substrate `ate-api-server` and `atelet` identities, as described in the chart prerequisites. Point DNS at your gateway. With `gateway.enabled: false`, route your HTTPS gateway to `actors-terse.actors.svc.cluster.local:7100`. Configure the controller's WebSocket timeout through your gateway configuration. The public origin is required even when the chart does not create ingress.

## Connect and verify

Configure the backend:

```sh
export DURABLE_ACTORS_CONTROL_PLANE_URL=https://actors.example.com
export DURABLE_ACTORS_PROJECT_ID=my-project
export DURABLE_ACTORS_SECRET="$(cat api-key)"
curl --fail https://actors.example.com/healthz
```

Use the [deployment registration API](reference/openapi.md) to register compiled actor code and its contract for your project, then generate your application client using the [TypeScript](reference/typescript-guide.md) or [Python](reference/python-guide.md) guide. A healthy controller does not register or deploy an application automatically. Upload immutable compiled artifacts into the configured bucket and register their manifest; artifact format and registration endpoints are described in the [OpenAPI specification](reference/openapi.yaml).

Verify an actor write and read, restart its sandbox, and read the value again. Also verify a WebSocket connection through your chosen ingress. Control-plane replicas handle HTTP and sockets by default; actors restore from prepared snapshots on shared workers. Increase `substrate.worker.autoscaling.minReplicas` for ready capacity and set the maximum to fit available node capacity. These settings do not create additional node pools or guarantee capacity.

## Speed up writes with GCS Rapid

Rapid stores the hot write log in two zones and keeps permanent history in the Standard bucket. Each acknowledged write has durably flushed to both Rapid copies. This can reduce state-write latency; benchmark your workload and account for the extra storage and request costs.

Choose a region with Rapid availability and use Google Cloud CLI 553 or newer. Create two distinct buckets once, then grant object access and bucket-metadata access to the controller identity. The following placements are examples; verify current [Rapid availability and requirements](https://docs.cloud.google.com/storage/docs/rapid-bucket).

```sh
export RAPID_A=my-unique-actor-rapid-a
export RAPID_B=my-unique-actor-rapid-b

gcloud storage buckets create "gs://${RAPID_A}" --project="$PROJECT" \
  --location=us-west4 --placement=us-west4-a --default-storage-class=RAPID \
  --enable-hierarchical-namespace --uniform-bucket-level-access --public-access-prevention
gcloud storage buckets create "gs://${RAPID_B}" --project="$PROJECT" \
  --location=us-west4 --placement=us-west4-b --default-storage-class=RAPID \
  --enable-hierarchical-namespace --uniform-bucket-level-access --public-access-prevention
for bucket in "$RAPID_A" "$RAPID_B"; do
  gcloud storage buckets add-iam-policy-binding "gs://${bucket}" \
    --member="serviceAccount:${GSA}" --role=roles/storage.objectUser
done
for bucket in "$BUCKET" "$RAPID_A" "$RAPID_B"; do
  gcloud storage buckets add-iam-policy-binding "gs://${bucket}" \
    --member="serviceAccount:${GSA}" --role=roles/storage.legacyBucketReader
done
```

Select Rapid in the installation values:

```yaml
storage:
  authorityBucket: my-unique-actor-state
  artifactBucket: my-unique-actor-state
  archiveBucket: my-unique-actor-state
  mode: rapid
  rapid:
    buckets:
      - {bucket: my-unique-actor-rapid-a, zone: us-west4-a}
      - {bucket: my-unique-actor-rapid-b, zone: us-west4-b}
```

The Standard bucket remains required. Startup checks Rapid storage class, actual zonal placement, and retention rules. Do not configure automatic deletion of Rapid log objects: the runtime archives them before removing their Rapid copies. If Rapid streams cannot be opened at activation, the runtime can persist immutable Standard snapshots.

Storage mode and bucket identities are bound to existing actor ownership records. Select the mode before creating actors in an installation. Changing an existing installation's mode needs a planned data migration; a Helm values change alone does not migrate actor state.

## Upgrades and troubleshooting

Pin a release version, review its compatibility notes, and repeat the install command with your saved values. Keep bucket identities and the credentials Secret stable. Clients should reconnect after controller replacement. Kubernetes rollback does not undo database migrations or changes to persisted data.

```sh
kubectl -n actors get pods
kubectl -n actors logs deployment/actors-terse -c control-plane --since=10m
kubectl -n terse-substrate get pods
kubectl -n terse-substrate get events --sort-by=.lastTimestamp
```

- Pending workers or rejected activations: check worker reservations, node capacity, and actor resource requests.
- Storage permission errors: check bucket roles, the Google service account annotation, and the Workload Identity binding.
- Substrate connection failures: check the projected API token, trust bundle, worker selectors, and snapshot bucket access.
- Controller startup failures: check PostgreSQL reachability, migration permissions, and the signing-key format.
- WebSocket disconnects: check ingress upgrade support and connection timeouts.

See the [chart values](../charts/durable-actors/values.yaml) for resource limits and optional settings.
