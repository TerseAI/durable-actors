# GKE Sandbox, Rust, and configurable durability

Updated September 28, 2026. The objective is lower activation and mutation latency without introducing an ownership database that we must shard as the actor fleet grows. The chosen production default is GKE Sandbox with Rust infrastructure, GCS ownership, direct GCS artifact delivery, and Rapid persistence. PostgreSQL ownership migration is outside the plan. Stronger durability is configurable. Rapid is the only production state-persistence and replication implementation. Backward compatibility is not a requirement; this supersedes the earlier requirement to support the current setup alongside the new architecture. Do not add arbitrary artifact or state-size caps.

The companion [benchmark report](gcs-rapid-benchmark.md) records the isolated experiment and the estimate against the recent PR 101 measurements. Implementation is in progress in an isolated worktree; production has not been migrated.

The [two-zone Rapid follow-up](gcs-rapid-multizone-benchmark.md) measured 18,000 writes with the Rust SDK under GKE Sandbox. Waiting for durable flushes in both zones took 3.0 ms p50 for 64-byte records, 3.2 ms for 4 KiB, and 4.2 ms for 64 KiB across both origins and rounds. This supports replacing passive sandbox storage replicas in the new configuration with persistent append streams to Rapid buckets in distinct zones. The benchmark verifies storage access, persisted offsets, readback, and writer-exit recovery; production fencing, partial-write recovery, and membership changes still require implementation and failure tests.

The [cross-region follow-up](gcs-rapid-crossregion-benchmark.md) also passed: 4,500 measured writes between Las Vegas and Northern Virginia, with 24 verified log readbacks and recovery of all 100 acknowledged versions from Virginia after deleting the Las Vegas probe. Two-region 4 KiB writes took 63–72 ms p50 and 64–74 ms p95; 64 KiB writes took 97–99 ms p50 and 101–108 ms p95. This is the synchronous distance cost for that pair, not the default policy latency. All production actor-state replication policies will use Rapid.

**One production persistence implementation**

| Concern | Production architecture |
| --- | --- |
| Execution | Google GKE Sandbox, Rust host and control plane, Bun worker |
| Customer code | Rust streams immutable deployment artifacts from GCS onto the sandbox |
| Ownership | Retain GCS conditional ownership, with Rapid recovery references |
| State persistence | Persistent Rapid append logs, with copies in the policy's configured buckets |
| Acknowledgement | One zonal flush by default; every required bucket for stronger durability |

Use one Rust Rapid implementation for zonal, regional, and cross-region policies. The policy selects bucket placement and required acknowledgements. Remove the passive sandbox storage replicas and the Standard GCS actor-snapshot persistence path from the new production architecture. Do not retain them as fallback replication implementations. GCS ownership records and immutable code artifacts remain separate from actor-state replication.

Keep sandbox provisioning, artifact delivery, ownership, and state persistence as separate effectful boundaries with injected implementations. Store the required bucket set, verified zone/region placement, and policy with the actor's ownership epoch. Configuration changes must not reinterpret an existing log or silently weaken an acknowledgement. The production runtime and container image have no Go dependency.

**Breaking replacement**

Update the SDK, configuration, deployment contract, ownership-record schema, and persisted-state format together wherever the new architecture requires it. Remove the Modal/Go production path, sandbox storage replicas, Standard GCS state-snapshot backend, and compatibility adapters from the delivered stack. Keep the GCS conditional-ownership mechanism; its existing record format need not be preserved. Inject narrow dependencies for testability without retaining obsolete production implementations.

Reading old storage formats, accepting old deployment manifests, and shipping migration tooling are outside this PR. Any transfer of existing production data is a separate rollout decision before traffic changes. This does not authorize deleting existing data, deploying production, or changing DNS.

**Default production architecture**

Use a regional GKE cluster with ordinary nodes for the trusted Rust control plane and Google's managed **GKE Sandbox** for customer workloads. Sandbox Pods use `runtimeClassName: gvisor`; Google supplies the sandbox runtime. The prototype uses GKE Standard with a dedicated sandbox node pool. [GKE Sandbox](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/sandbox-pods)

The Rust control plane manages sandbox lifecycle through the Kubernetes API, using the Rust `kube` client. Keep generic sandboxes warm, with Bun and the SDK worker initialized before assignment. A control-plane claim assigns an existing sandbox to one actor. Customer execution permanently consumes that sandbox; delete it after use and replenish the pool. Node provisioning and Pod creation belong outside the normal activation path. A spare becomes assignable only after its Rust host and Bun worker have completed warming; defer only actor-specific code, state, and authority work until assignment. Keep readiness in the pool registry so assignment does not wait for a fresh readiness-probe cycle. Connect directly from the in-cluster control plane using reused clients; use the Kubernetes API for lifecycle management, not the actor invocation data path. Use a shared gateway for HTTP and WebSockets, routing to private Pod addresses.

GKE Agent Sandbox is an optional Google-provided controller for Sandbox resources and warm pools. Evaluate it against the small set of lifecycle operations already required by this system; do not add a second competing spare allocator. The runtime choice is GKE Sandbox either way. [Agent Sandbox on GKE](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/how-install-agent-sandbox)

| Operation | New behavior |
| --- | --- |
| Build | Produce deployment artifacts and upload immutable objects to GCS; record artifact references and integrity metadata. |
| Prepare spare | Create a GKE Sandbox Pod; initialize Rust host and Bun worker; advertise readiness. |
| Activate | Claim ownership and a ready spare through the control plane; stream artifacts onto the sandbox while recovering state and preparing persistence. |
| Invoke | Route directly to the assigned host; execute in Bun; wait for the selected durability acknowledgement. |
| Stop | Fence and stop execution, finish the required persistence/handoff, remove routing, and delete the Pod. |
| Repair | Reconcile Kubernetes objects, ownership, and replica placement; restore the required failure-domain copies before accepting writes that require them. |

**Control plane and routing**

Run the Rust control plane in the GKE cluster on ordinary trusted nodes, with replicas spread across zones. Customer execution uses GKE Sandbox nodes. A shared external HTTPS load balancer will serve `actors.useterse.ai`; the gateway forwards HTTP and WebSocket traffic to private sandbox addresses using authenticated routing. Do not create a public load balancer per actor. DNS and production traffic remain unchanged until rollout.

Colocation can remove public routing overhead on control-plane-to-sandbox requests. It does not erase client-to-region latency. The previous approximately 20 ms activation saving is a planning estimate, not a measured gain from the final gateway implementation. Hot calls need no ownership-database read or write solely to route a cached, authenticated target.

**Rust downloads code directly**

Store deployment artifacts in regional Standard GCS. The Rust host reads objects directly with the official SDK and streams their chunks to local sandbox files, verifying their digests and publishing the completed artifact before Bun imports it. Fetch independent artifact files concurrently. Code download, state recovery, and opening the new persistence stream run concurrently where their dependencies permit.

The default path uses direct streaming downloads without a filesystem mount or added artifact-size cap. Resource and provider failures propagate. The benchmark uses the same small Counter artifact as the recent measurements to isolate architecture changes; that fixture size does not define a product limit.

Keep immutable code recoverable independently of the actor's state durability setting. A regional code origin survives a zone loss; a cross-region recovery promise also requires the code to be available outside the failed region before the deployment is considered ready. Do not use a GCS filesystem mount for activation. [GKE Sandbox storage restrictions](https://docs.cloud.google.com/kubernetes-engine/docs/concepts/cloud-storage-fuse-csi-driver)

**GCS ownership scales independently across actors**

Keep authority records in regional Standard GCS while moving actor-state persistence to Rapid. Preserve the generation-conditional ownership claims, lease checks, renewal, and fencing mechanism. Separate metadata storage from state persistence, and adapt recovery references to actor/epoch-specific Rapid segments. This reduces the scope of the authority change but still requires integration and recovery tests. Conditional ownership updates continue using GCS generation preconditions. [GCS preconditions](https://docs.cloud.google.com/storage/docs/request-preconditions)

The saved traces model a returning write at roughly 325–360 ms p50 with GCS ownership, versus 242 ms with PostgreSQL; the 80–115 ms difference depends on whether Rapid stream setup safely overlaps the claim. Keeping the existing sequence and opening the stream only after the claim gives the conservative 360 ms estimate. Normal hot read/write latency is essentially unchanged by this ownership choice. These hybrid estimates are modeled, not a measured GCS-plus-Rapid run; see the companion benchmark report. Accept this activation tradeoff to avoid operating and repartitioning an ownership database. PostgreSQL remains a benchmark comparison, not a planned migration.

Ownership is one independent conditional record per actor. Keep the control plane horizontally replicated, with no global ownership manifest or process that must serialize all actors' claims. Cache routes as hints; the conditional claim and local lease fence remain authoritative. Publish the route only after the assigned host is ready. Warm calls use the established route and lease, without a remote ownership write for every mutation.

Background work matters to capacity even when it is off the request path. The current default renews a 30-second lease every 10 seconds. With N active actors, periodic renewal alone produces roughly N/10 ownership writes per second; the current renewal also reads the owner before its conditional write. One million active actors would therefore produce about 100,000 reads and 100,000 writes per second before activation, release, retries, and inventory changes. This is arithmetic from the current protocol, not a measured capacity result. PostgreSQL read replicas cannot absorb those writes; scaling beyond one writer requires an additional partitioning design. [Cloud SQL replication](https://docs.cloud.google.com/sql/docs/postgres/replication)

GCS automatically distributes increasing request load across servers, but redistribution takes time. Use object-key distributions that avoid concentrating new actors in a sequential key range, and test both gradual growth and bursts. Its documented initial capacity of roughly 1,000 writes and 5,000 reads per second per bucket is not a fixed bucket ceiling. [GCS request scaling](https://docs.cloud.google.com/storage/docs/request-rate)

The separate documented limit of one write per second to the same object name applies to the Standard GCS ownership record. Periodic 10-second renewals fit that rate; rapid acquire/release cycles, competing claims, and inventory-triggered renewals can still encounter throttling. Measure that behavior and preserve fencing on failed or ambiguous updates. This is a provider constraint to test, not a new product cap. [GCS object limits](https://docs.cloud.google.com/storage/quotas)

Keeping ownership in GCS does not by itself solve fencing. The Rust runtime must prevent stale owners from acknowledging mutations. Rapid segments must belong to a specific actor/epoch and recovery must ignore obsolete writers. Lease expiry, renewal, acknowledgement races, recovery after a crash, and membership replacement need explicit behavior tests. A successful Rapid append alone is not proof that the writer still owns the actor.

Keep ownership metadata in regional Standard GCS for the regional profile. Cross-region recovery additionally needs a design for ownership metadata outside the failed region; asynchronous copies alone do not establish zero data loss or safe takeover. The existing PostgreSQL comparison used one instance and does not establish fleet-scale or HA performance.

**Durability settings describe the acknowledgement**

| Setting | Return success after | Failure protection |
| --- | --- | --- |
| Zonal, default | A server-confirmed flush on a persistent Rapid stream, selected from the experiment | Process/node failure within the surviving zone; loss of the zone can lose acknowledged state |
| Regional | A durable flush in every required Rapid bucket, spanning at least two zones in one region | Loss of one zone, provided ownership, code, and recovery metadata also survive |
| Multi-region | A durable flush in every required Rapid bucket, spanning the required regions | Loss of a region; includes cross-region network latency |

A background copy never upgrades the guarantee of an earlier acknowledgement. Do not let one Rapid bucket satisfy a regional policy. If the required acknowledgements cannot be obtained, fail the write. For the Rapid configuration, every required copy lives in a bucket with explicit zone/region placement; multiple buckets in the same zone do not satisfy the regional profile. Keep these logs available for recovery independently of whether the customer actor is executing. Recreating failure-domain protection does not claim Google's numerical storage durability SLA. [Storage durability](https://docs.cloud.google.com/storage/docs/availability-durability)

For the low-latency default, keep replication/checkpointing to other failure domains asynchronous when configured. For stronger modes, the required remote copies are synchronous. Never silently weaken the policy during a fault.

**Latency forecast**

For an already active actor, established Rapid streams, a trivial method, and a 4 KiB persisted record:

| Policy | Measured persistence p50 | Measured persistence p95 | Predicted client hot-write p50 | Predicted client hot-read p50 |
| --- | ---: | ---: | ---: | ---: |
| Zonal, one bucket | About 3 ms | About 5–7 ms | About 75 ms | 35–40 ms |
| Regional, two zones in us-west4 | About 3–4 ms | About 6–8 ms | About 75–80 ms | 35–40 ms |
| Multi-region, us-west4 + us-east4 | 63–72 ms | 64–74 ms | About 135–145 ms | 35–40 ms |

The client predictions preserve the saved laptop benchmark's routing and request overhead. They replace its persistence stage with the measured Rapid flush cost. The baseline hot-write total minus its commit stage has a median of about 72 ms; adding the measured flush gives these planning estimates. This is a component model, not a measured integrated request distribution or a production SLO. It assumes one concurrent flush round on the critical path. Additional sequential persistence rounds required by the final correctness protocol must be added, especially across regions. No unmeasured gateway saving is credited.

The two-zone test's pooled 4 KiB p50 values were 3.350 ms local and 3.244 ms with both copies; that small reversal is experimental variability. Allow a small regional increment instead of treating replication as a speedup. Reads of an active actor's resident state do not wait for storage copies. Cross-region placement adds latency to synchronous writes, not a storage round trip to every hot read.

Record size and region pair matter. In the same cross-region experiment, 64-byte records took about 79–80 ms p50 to persist and 64 KiB records about 97–99 ms. The corresponding client hot-write estimates are about 150–155 ms and 170–175 ms. The separate streams and short experiment do not establish a monotonic size/latency curve. These sizes are benchmark inputs, not limits. Adding more required buckets can increase the slowest flush and tail latency; the two-copy measurements do not benchmark arbitrary replica counts.

Activation is a separate path. The existing GCS-ownership model gives about 325–360 ms p50 for a returning zonal write with a warm spare, a small artifact/state, and the previous routing overhead. A roughly 20 ms colocation benefit remains an unverified planning estimate. Two-zone activation may be close if stream setup overlaps, but has not been integrated or measured. Cross-region activation also opens remote streams and may require remote recovery or fencing, so adding only the hot-flush difference is insufficient. The cross-region recovery probe observed 180 ms for a fresh local read in Virginia and 1.95 seconds for one fresh remote read of the Vegas log; those single observations are not activation percentiles. The initial 3.8–5.0 second fresh-Pod observations include two-container startup, a 2-second readiness-probe cadence, laptop polling, and a proxied worker-warming request. They measure background replenishment and do not isolate gVisor startup. The 325–360 ms forecast retains about 189 ms of old-path residual overhead; it is not a measured latency floor for the colocated GKE architecture. See the [lifecycle audit](gcs-rapid-benchmark.md) for the exact harness behavior and Google's published warm-pool allocation figures.

**Replication and recovery invariants**

Persist versioned, checksummed records with an ownership epoch and stable operation identity. Send the same record to every required Rapid stream concurrently and return success only after all required persisted offsets confirm it. A failed or ambiguous flush must not let the actor continue producing incompatible state under the same version.

Recovery must retain every acknowledged operation while handling complete but unacknowledged records and partial trailing frames. Do not equate the longest readable log with a proven commit point. Preserve the existing single-owner authority contract, enforce lease expiry during writes, and test takeover while an old writer is still running. Rapid's per-object single-writer behavior is not a replacement for the cross-bucket ownership protocol.

Membership and policy changes require a new fenced epoch and verified copies in the new required failure domains before the stronger policy can acknowledge writes. Two required buckets preserve acknowledged data after one fails, but cannot keep acknowledging the same two-copy policy until redundancy is restored. Never silently downgrade during that interval.

**Rapid and the Rust SDK**

The repository already pins the official `google-cloud-storage` crate at 1.17.0 and enables its `google_cloud_unstable_storage_bidi` compile gate. It has `open_appendable_object`, `reopen_appendable_object`, `append`, `flush`, and finalization. `append` can buffer; `flush` returns the server-confirmed persisted offset. Reuse a stream across mutations and open it during activation to overlap setup with code download and recovery. [Official Rust API source](https://raw.githubusercontent.com/googleapis/google-cloud-rust/main/src/storage/src/storage/client.rs)

Use the bidirectional object API for Rapid reads, with the persisted record range when recovering a log. Ordinary object reads and unfinalized appendable objects need separate testing; the smoke experiment exposed a multi-second read path. A requested record range describes the data to recover, not an arbitrary artifact-size restriction. Frame log records so recovery can distinguish a complete durable record from a partial write. [Rapid object operations](https://docs.cloud.google.com/storage/docs/rapid/use-objects-in-zonal-buckets)

API availability and latency are different questions. The Rust API works in the experiment, but its Rapid write interface is still compile-gated and Google's documented DirectPath client support does not establish Rust DirectPath support. Measure the actual Rust path under GKE Sandbox; do not substitute the advertised submillisecond storage figure. [Direct connectivity](https://docs.cloud.google.com/storage/docs/direct-connectivity)

**Implementation after the benchmark**

1. Storage feasibility is measured for both cross-zone and cross-region Rapid, including exact durable-flush offsets, full log readback, and independent recovery after primary compute termination. Use the saved results as the baseline for integrated validation.
2. Implement the Rust GKE lifecycle, warm pool, colocated control plane, shared HTTP/WebSocket gateway, and direct streaming artifact download. Use Google's managed GKE Sandbox runtime and a Go-free production image.
3. Keep GCS ownership and adapt recovery references to Rapid. Implement a single Rust Rapid log backend with concurrent writes to the required bucket set, replacing the in-progress proposal to combine Rapid with sandbox replicas or a Standard snapshot fallback.
4. Add policy validation, epoch fencing, partial-write recovery, safe membership changes, log rotation/recovery, and the new storage/configuration contract. Use range reads and log metadata so recovery does not require rereading an unbounded log from its beginning. Do not add arbitrary artifact or state-size caps.
5. Follow TDD for the production protocol and exercise lost replies, stale owners, mid-write crashes, inaccessible buckets, zone/region loss, and membership changes. Measure the complete GCS-ownership-plus-Rapid activation path and the shared gateway, along with loaded throughput and realistic artifact/state sizes.
6. Deliver the implementation as a reviewable PR from the isolated worktree. Deliver a breaking replacement without legacy compatibility or migration tooling. Production rollout is a separate step after integrated validation; do not deploy or change DNS in this PR.

The storage experiments establish API feasibility and observed latency. They do not establish production fencing, full-region actor availability, or a production latency SLO. Cross-region state durability still requires a separate decision for ownership metadata and code availability during loss of the owner region.
