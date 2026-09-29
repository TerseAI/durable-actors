# Terse on GKE Sandbox

This chart runs the Rust control plane and HTTPS/WebSocket gateway in Kubernetes. The control plane maintains a pool of ready GKE Sandbox pods containing Rust and a prewarmed Bun worker. Assignment streams an immutable GCS code artifact into `/customer` while ownership and state recovery run in parallel. The normal activation path claims a ready pod; Kubernetes scheduling and image pulls happen during pool replenishment.

Every state write is sent concurrently to the configured Rust storage replicas and acknowledged only after **all replicas commit it to disk**. There is no quorum or consensus service. Replicas archive compressed change records to Standard GCS every 16 MiB or 10 seconds, whichever comes first. GCS retains ownership CAS and host leases; PostgreSQL holds deployments, traces, and spare bookkeeping.

The default is three replicas in one zone for low latency, each on a distinct node with its own persistent disk. The placement list determines the replica count; there is no fixed maximum. Select multiple zones or regions for stronger failure protection. An unavailable required replica stops writes.

## Prerequisites

- A GKE Standard cluster with Workload Identity Federation, VPC-native networking and enforced NetworkPolicy (GKE Dataplane V2 is recommended). Enable the Gateway API if using the bundled public gateway.
- Regular node pools for the control plane and storage replicas and COS GKE Sandbox node pools in every configured compute zone. Google manages gVisor through `runtimeClassName: gvisor`; the provider selects `sandbox.gke.io/runtime: gvisor`. Nodes need registry pull access and outbound Google API access.
- A regional Standard GCS authority bucket, an artifact bucket, and a Standard GCS archive bucket. Use uniform bucket-level access and public access prevention. Do not apply object deletion lifecycle rules to live state, ownership or deployed code.
- A PostgreSQL database reachable from the control plane. Its user must run schema migrations. Its availability, the ownership bucket and the code bucket are separate from state durability.
- A Google service account with `roles/storage.objectUser` on the authority, artifact and archive buckets, plus `storage.buckets.get` on the archive bucket for Standard storage validation (for example through `roles/storage.legacyBucketReader`). Bind the chart's Kubernetes service account (`<release>-terse`) to it with `roles/iam.workloadIdentityUser`. The application exchanges its credential for scoped GCS credentials; customer pods have no Kubernetes service account token or Google identity.
- An existing Secret in the control namespace containing `postgres-url`, `api-key`, `replica-key` (at least 32 random bytes), and `jwt-signing-key` (base64 encoded Ed25519 PKCS#8). All control-plane replicas must share these values and the same storage/pool configuration.
- For the public gateway: an existing Kubernetes TLS Secret matching the hostname in `publicUrl`. Optionally reserve a global address and set `gateway.addressName`.

The chart creates a dedicated sandbox namespace and grants the control plane pod lifecycle, exec and Secret-read permissions there. Customer Secrets must carry `terse.ai/customer-secret: "true"`; reference their Kubernetes names in deployment `secretRefs`. Do not put infrastructure credentials in that namespace. Network policies permit sandbox traffic to the control plane, authenticated storage replicas, cluster DNS and the public internet, and block private networks and metadata endpoints. DNS egress permits both `kube-dns` and `node-local-dns` pods in `kube-system`. For DNS listeners using host networking or a link-local address, configure the resolver address in `networkPolicy.dnsCidrs`.

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

## Durability and placement

| Mode | Placement | Acknowledgement | Failure protection for acknowledged state |
| --- | --- | --- | --- |
| `zonal` (default) | One or more replicas in one zone | All replicas | Independent node/disk failures while another copy survives |
| `regional` | Replicas in at least two zones in one region | All replicas | Loss of a zone |
| `multi_region` | Replicas in at least two regions | All replicas | Loss of a replica region while a remote copy survives |

```yaml
storage:
  authorityBucket: actor-ownership
  artifactBucket: actor-code
  archiveBucket: actor-archive
  durability: regional
  replicas:
    placements: [us-west4-a, us-west4-b, us-west4-c]
    storageClassName: premium-rwo
    diskSize: 100Gi
```

Google GKE clusters do not span regions. For multiple regions, deploy the selected replica indices in their respective clusters using `storage.replicas.deployIndices`, and configure the same complete `placements` and `addresses` arrays everywhere. Addresses must be reachable between clusters and from the control plane and sandboxes. Provide private routing/TLS, expose each remote replica through a stable private endpoint, and add the remote pod/endpoint networks to `networkPolicy.replicaCidrs` for sandbox egress and replica ingress. The chart cannot create a cross-region network by scheduling pods into a remote zone.

Membership is fixed for existing actors and recorded in GCS ownership. Reordering, adding, removing or moving replicas is not an automatic migration. Configuration mismatches fail closed; create a new deployment and explicitly migrate data when changing membership. Restarting an existing replica with its retained disk keeps its identity and membership unchanged.

## Recovery and archival

Each replica uses SQLite WAL with `synchronous=FULL`. It stores exact actor snapshots as compressed change records, using the previous snapshot as a compression dictionary when smaller. Archive batches start with an independently decodable checkpoint. This preserves byte identity while avoiding repeated unchanged bytes in network traffic and archived data; it does not change the actor runtime's JSON snapshot API into SQLite transactions.

The first configured replica normally archives at 16 MiB or 10 seconds, checked on a 100 ms timer. The size is a batching target, not an object or actor size limit. A single large record is accepted. Other replicas verify archive notifications against GCS before discarding their pending records. If the primary uploader fails, another replica starts archival after 30 seconds. Concurrent/retried uploads are immutable and content-addressed. The timer is not a data expiry: upload failures retain records on disk and retry. Storage exhaustion fails writes.

Actor recovery waits for the old GCS lease to expire, durably seals at least one registered replica for that epoch, and reads surviving copies. A registered disk that never prepared that epoch can durably fence it and prove that it never acknowledged a write. Sealing prevents the old owner from obtaining another all-replica acknowledgement. Missing or conflicting recovery evidence fails closed. Unknown write outcomes may be visible after recovery; a failed request does not prove that its mutation was never persisted. A surviving replica and the GCS authority must be reachable; having durable state does not guarantee immediate availability.

Persistent-volume retention and disk identity registration prevent an empty disk from silently replacing a previously acknowledged copy. Restore a lost replica as follows:

1. Stop the affected replica and preserve its stable ID; do not delete its GCS disk registration. Ensure no old process can continue serving that identity.
2. Mount its replacement PVC in a one-off pod using the same runtime image, service account, storage configuration and `replica-key`.
3. Set `DURABLE_ACTORS_PROCESS_ROLE=replica_restore`, `DURABLE_ACTORS_REPLICA_ID` to the affected ID, `DURABLE_ACTORS_REPLICA_DATA` to the new database path, and `DURABLE_ACTORS_RESTORE_SOURCE_ID` to a surviving configured replica.
4. Run the binary. It seals the source's existing epochs before exporting a consistent disk image, restores all pending logs/checkpoints, and preserves the registered destination identity. It refuses to overwrite a populated destination.
5. Stop the restore pod and restart the original StatefulSet against that PVC. Old actor epochs remain sealed; actors reacquire ownership before writing again.

This restore intentionally fences active epochs in the shared source replica. Schedule it as a recovery operation. No safe replica is available when every unarchived copy has been destroyed; archived data alone cannot reconstruct acknowledged writes that never reached GCS.

Replica StatefulSets use `OnDelete` updates, retained PVCs and a zero-disruption PDB. Plan maintenance explicitly: all-copy acknowledgement trades write availability for a simple durability contract. Archive history is currently retained indefinitely. Do not add a blanket 30-day deletion rule: old checkpoints may still be referenced by dormant actors. The inspection API can replay archived versions; retention/compaction must preserve referenced bases.

## Code deployment and capacity

Build a source image extending the published runtime image, copy the application source and installed dependencies into it, push it, then deploy its digest. The provider runs the bundled compiler inside a temporary GKE Sandbox pod, publishes the resulting `actors.mjs` or `actors.pyz` and Python dependencies to the artifact bucket, and records a generation-pinned, SHA-256 checked manifest. Actor pods use the shared runtime image and receive code over the Rust GCS client; they do not mount GCS filesystems.

```dockerfile
FROM RUNTIME_IMAGE_AT_SHA256_DIGEST
COPY --chown=10000:10000 . /customer
WORKDIR /customer
```

Use the existing deployment API with an OCI digest in `imageRef`, `/customer` as `workingDirectory`, and the source entrypoint. The bundled compiler needs the application dependencies available in the source image. TypeScript produces a bundled JavaScript module; separately loaded JavaScript files/native addons need explicit packaging support before using them. Python publishes its source archive and installed dependency tree. Python assignment currently starts its executor after claiming a prewarmed Bun pod, matching the existing Python runtime behavior; it adds interpreter startup latency.

`pool.idle`, `pool.fleetMaximum`, `pool.maxStarting`, CPU and memory control prewarming. Defaults maintain 64 ready spares per runtime/region, permit 32 concurrent starts, and cap the unassigned spare fleet at 256; active actors do not consume this spare budget. Each default actor requests and is limited to 0.25 CPU and 256 MiB. Keep enough nodes and ready spares for bursts. Resource overrides that differ from the configured pool currently create a pod on demand; these requests include pod startup latency. Running out of ready spares also uses this cold path. Large code and state payloads have no application byte ceiling; node/container resources and upstream service limits still apply.

Configure node-pool autoscaling separately from the chart. Actor pods permit cluster-autoscaler eviction; the control plane reconciles missing warm and active pods every 30 seconds. Eviction can interrupt calls and sockets, and the next activation recovers acknowledged state from the storage replicas. Keep storage replicas on a separate node pool and plan their maintenance explicitly.

Validate the assembled architecture on staging before production cutover: workload identity, persistent-volume provisioning, warm invocations, node loss, paused uploads, restore, actor fencing, and cross-region routing.

References: [GKE Sandbox](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/sandbox-pods), [GKE Gateway TLS](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/secure-gateway).

For a Google-managed Compute Engine SSL certificate, set `gateway.preSharedCert` to its name and `gateway.tlsSecret` to an empty string. The certificate must be global for the default gateway class. Set `gateway.addressName` to a reserved global address and point the public hostname at that address.
