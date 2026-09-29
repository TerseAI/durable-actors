# Terse on GKE Sandbox

This chart runs the Rust control plane and HTTPS/WebSocket gateway in Kubernetes. The control plane maintains a pool of ready GKE Sandbox pods containing Rust and a prewarmed Bun worker. Assignment streams an immutable GCS code artifact into `/customer` while ownership and state recovery run in parallel. The normal activation path claims a ready pod; Kubernetes scheduling and image pulls happen during pool replenishment.

State is appended concurrently to the configured Rapid buckets. An acknowledged write has flushed to **every configured bucket**. The default is zonal durability. Standard GCS retains ownership CAS and host leases; PostgreSQL holds deployments, traces, and spare bookkeeping. There are no storage replica sandboxes or external sandbox provider processes.

## Prerequisites

- A GKE Standard cluster with Workload Identity Federation, VPC-native networking and enforced NetworkPolicy (GKE Dataplane V2 is recommended). Enable the Gateway API if using the bundled public gateway.
- A regular node pool for the control plane and COS GKE Sandbox node pools in every configured compute zone. Google manages gVisor through `runtimeClassName: gvisor`; the provider selects `sandbox.gke.io/runtime: gvisor`. Nodes need registry pull access and outbound Google API access.
- A regional Standard GCS authority bucket, an artifact bucket, and Rapid buckets with hierarchical namespace in the desired zones. Use uniform bucket-level access and public access prevention. Do not apply object deletion lifecycle rules to live state, ownership or deployed code. Confirm Rapid availability in each selected zone.
- A PostgreSQL database reachable from the control plane. Its user must run schema migrations. Its availability, the ownership bucket and the code bucket are separate from state durability.
- A Google service account with `roles/storage.objectUser` on the authority, artifact and Rapid buckets, plus `storage.buckets.get` on Rapid buckets for startup placement validation (for example through `roles/storage.legacyBucketReader`). Bind the chart's Kubernetes service account (`<release>-terse`) to it with `roles/iam.workloadIdentityUser`. The application exchanges its credential for scoped GCS credentials; customer pods have no Kubernetes service account token or Google identity.
- An existing Secret in the control namespace containing `postgres-url`, `api-key`, and `jwt-signing-key` (base64 encoded Ed25519 PKCS#8). All control-plane replicas must share these values and the same storage/pool configuration.
- For the public gateway: an existing Kubernetes TLS Secret matching the hostname in `publicUrl`. Optionally reserve a global address and set `gateway.addressName`.

The chart creates a dedicated sandbox namespace and grants the control plane pod lifecycle, exec and Secret-read permissions there. Customer Secrets must carry `terse.ai/customer-secret: "true"`; reference their Kubernetes names in deployment `secretRefs`. Do not put infrastructure credentials in that namespace. Network policies permit sandbox traffic to the control plane, cluster DNS and the public internet, and block private networks and metadata endpoints. If the cluster uses NodeLocal DNS, configure its DNS address in `networkPolicy.dnsCidrs`.

## Install

Start with a copy of `values.yaml`. Set the immutable runtime image digest, bucket names, Google service account, namespaces, and the desired zones. The image must match the SDK/compiler version used by deployments.

```sh
helm lint charts/terse -f production-values.yaml
helm template actors charts/terse --namespace terse-control -f production-values.yaml
helm upgrade --install actors charts/terse --namespace terse-control --create-namespace -f production-values.yaml
kubectl -n terse-control get gateway actors-terse
```

Point `actors.useterse.ai` at the provisioned Gateway address after certificate and routing checks pass. This command does not change DNS. With `gateway.enabled: false`, route an existing HTTPS/WebSocket load balancer to the control-plane Service. Health checks use `/healthz`; long-lived sockets require suitable backend timeouts.

The chart is packaged as a CI artifact. `helm package charts/terse` produces a standalone release archive. It does not create the cluster, node pools, Google buckets, PostgreSQL instance, Google IAM bindings, or TLS certificate.

## Durability policies

| Policy | Required copies | Acknowledgement | Protects acknowledged state against |
| --- | --- | --- | --- |
| `zonal` (default) | At least one bucket in one zone | Every configured bucket | Rapid's redundancy within that zone |
| `regional` | Buckets in at least two zones in one Google region | Every configured bucket | Loss of one configured zone |
| `multi_region` | Buckets in at least two Google regions | Every configured bucket | Loss of one configured region |

For regional durability:

```yaml
storage:
  durability: regional
  rapidBuckets:
    - {name: actors-west-a, zone: us-west4-a}
    - {name: actors-west-b, zone: us-west4-b}
```

For multi-region durability:

```yaml
storage:
  durability: multi_region
  rapidBuckets:
    - {name: actors-west-a, zone: us-west4-a}
    - {name: actors-east-a, zone: us-east4-a}
```

Every compute zone needs a local copy. Additional copies may be remote; Rapid permits access from other Google zones and regions. Writes are sent in parallel, so commit latency follows the slowest required copy. A missing copy fails writes rather than silently lowering durability. Recovery reads surviving copies and rejects conflicting records; an ambiguous write can appear after recovery even if its caller never received success. State durability does not imply uninterrupted writes during a bucket/zone/region outage.

Policy and membership are fixed for an installation and recorded with actor ownership. This release deliberately rejects mismatched storage configurations; changing the chart is not a data migration. Use a fresh PostgreSQL database/schema and authority/state namespace for this breaking deployment, and explicitly migrate existing data before cutover. There are no compatibility readers for the previous runtime.

## Code deployment and capacity

Build a source image extending the published runtime image, copy the application source and installed dependencies into it, push it, then deploy its digest. The provider runs the bundled compiler inside a temporary GKE Sandbox pod, publishes the resulting `actors.mjs` to the artifact bucket, and records a generation-pinned, SHA-256 checked manifest. Actor pods use the shared runtime image and receive code over the Rust GCS client; they do not mount GCS filesystems.

```dockerfile
FROM RUNTIME_IMAGE_AT_SHA256_DIGEST
COPY --chown=10000:10000 . /customer
WORKDIR /customer
```

Use the existing deployment API with an OCI digest in `imageRef`, `/customer` as `workingDirectory`, and the source entrypoint. The bundled compiler needs the application dependencies available in the source image. It produces a bundled JavaScript module; separately loaded files/native addons need explicit packaging support before using them.

`pool.idle`, `pool.fleetMaximum`, `pool.maxStarting`, CPU and memory control prewarming. Keep enough nodes and ready spares for bursts. Resource overrides that differ from the configured pool currently create a pod on demand; these requests include pod startup latency. Running out of ready spares also uses this cold path. Large code and state payloads have no application byte ceiling; node/container resources and upstream service limits still apply.

Measured Rapid commit and activation primitives, methodology and estimates are in [the architecture plan](../../docs/research/gcs-rapid-kubernetes-plan.md). They are not an end-to-end production SLA. Validate the chart, workload identity, first activation, warm invocations, socket reconnects, host termination and remote-copy recovery in a staging cluster before production cutover.

References: [GKE Sandbox](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/sandbox-pods), [Rapid buckets](https://docs.cloud.google.com/storage/docs/rapid/rapid-bucket), [GKE Gateway TLS](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/secure-gateway).


## Building the runtime

The pinned Google Rust SDK exposes appendable objects behind its `google_cloud_unstable_storage_bidi` compiler gate. Repository and Docker builds load it from `.cargo/config.toml`. Builds from a published crate or another workspace must also set `RUSTFLAGS="--cfg google_cloud_unstable_storage_bidi"`. Keep the SDK lockfile and runtime image digest pinned; upgrades require append, flush, seal, recovery and failure testing.
