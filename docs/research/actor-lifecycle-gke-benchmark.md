# Live GKE actor lifecycle benchmark

Measured September 29, 2026 from the developer's Mac through a public Google HTTPS load balancer to the PR #107 deployment in `us-west4-a`. These measurements include the real SDK, Kubernetes control plane/gateway, GKE Sandbox with gVisor, GCS ownership and code download, and three storage replicas on separate nodes. The earlier [local component benchmark](actor-lifecycle-benchmark.md) excluded those cloud paths.

| Operation | Samples | Median | p95 |
| --- | ---: | ---: | ---: |
| Cold write | 20 | 227.8 ms | 267.7 ms |
| Cold read | 20 | 236.1 ms | 289.6 ms |
| Hot write | 200 | 40.8 ms | 92.1 ms |
| Hot read | 200 | 37.2 ms | 90.0 ms |
| Resume write | 20 | 312.9 ms | 372.6 ms |
| Resume read | 20 | 315.9 ms | 389.4 ms |

All 480 measured operations succeeded with the expected values. Another 48 warmup operations passed. Separate smoke runs before measurement and after packaging the reusable harness also passed; their timings are excluded from the table. No measured failures or slow samples were discarded. p50 is the median; p95 uses nearest rank. Twenty activation samples provide only a small view of tail behavior.

[Raw client/server samples](data/lifecycle-gke-20260929.csv) · [Configuration, validation, and summaries](data/lifecycle-gke-20260929.json)

## Deployment and workload

The release runtime was built from commit `25bc6ad`. The live chart included the NodeLocal DNS fix in `b3a53cd`. Image digests, GKE version, and complete timing summaries are recorded in the metadata file.

- Three Rust storage replicas, each on a different `e2-standard-4` node, each with its own 100 GiB `premium-rwo` SSD Persistent Disk. SQLite uses WAL and `synchronous=FULL`; every mutation waits for all three replicas.
- Two control-plane/gateway pods in the same cluster and zone. PostgreSQL 16 runs in the cluster on a separate 10 GiB SSD volume.
- Two `e2-standard-8` sandbox nodes running Google's gVisor runtime. Each actor receives 1 vCPU and 1 GiB. The idle pool targets five prewarmed sandboxes.
- Three regional Standard GCS buckets in `US-WEST4` for ownership, code, and archives. Archive batching remains 16 MiB or 10 seconds. The replica placement policy is **zonal**; this run does not measure cross-zone or cross-region writes.
- A public GKE Gateway terminates TLS. The temporary benchmark certificate was explicitly trusted by the client; certificate verification was enabled. Client: Node 24.19.0 on macOS 15.7.3. Actor: Bun 1.4.2.

The Counter actor reads or increments one persisted integer. Calls are serial. This is a small-state latency test, not a large-state, concurrency, throughput, or node/zone-failure test.

## What each case measures

**Cold** means the first invocation of a new actor on a prewarmed gVisor sandbox. Read and write use different actor IDs. Customer code is installed from GCS during activation. Generic process warmup, pod scheduling, image pulls, and source compilation happen before timing. Each request includes SDK routing, public HTTPS transport, control-plane work, code/state activation, actor execution, and receipt of the result. Two initial actors per case are warmups, so this does not represent the laptop's first DNS/TLS connection to a new endpoint.

**Hot** means repeated SDK calls against the resident actor. All 400 measured hot operations used a cached target, verified by SDK telemetry. They still pass through the public HTTPS gateway, and writes still wait for all three durable disk acknowledgements.

**Resume** uses the original client and returning actor ID after the default 10-second host idle timeout. Before measuring, the harness checks that the old host has stopped, its GCS lease is released, and its epoch is sealed. It waits for every replica to finish GCS archival and verifies zero pending records and released local payload/checkpoint blobs for that epoch. The request must restore the correct value on a different host with a higher epoch. Read and write again use separate actors. Idle and archive waiting are outside the invocation timer.

All 88 activations, including warmups, matched a pod UID observed ready before the request, with `runtimeClassName: gvisor`. All three replicas confirmed archival of all 44 initial epochs before the resume phase.

## Where the time goes

Correlated Rust logs measured assignment-to-ready medians of 144–148 ms for cold activation and 219–224 ms for resume. Control-plane target resolution, which contains that host activation work, took 177–178 ms cold and 265–270 ms on resume. These are overlapping intervals and must not be added together.

Inside the host's request instrumentation, hot writes took 6.6 ms median / 8.4 ms p95 and hot reads 1.6 ms median / 2.7 ms p95. A separate 50-request public `/healthz` baseline measured 32.3 ms median / 39.0 ms p95.

The measured end-to-end hot p95 remains around 90 ms. The much smaller host-request timings place most of that tail outside the instrumented actor execution interval; this run does not isolate which client, gateway, transport, or scheduling stage causes it. The public medians are the relevant laptop-facing numbers.

## Live fix and reproduction

The first spare pods failed before readiness because the chart permitted `kube-dns` but omitted GKE Dataplane V2's `node-local-dns` pods. A failing chart test reproduced the missing rule. The policy now permits TCP/UDP port 53 to both DNS pod types in `kube-system`; the diagnostic gVisor pod then resolved the control-plane service and the spare pool became ready. All 13 chart tests passed. See Google's [NodeLocal DNS documentation](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/nodelocal-dns-cache).

The reusable runner and setup contract are in [tests/gke-lifecycle](../../tests/gke-lifecycle/README.md). The isolated deployment was installed using the repository's Helm chart. Provisioning commands, Kubernetes placement/PV evidence, startup logs, and archive evidence are retained in the benchmark worktree under `.artifacts/lifecycle-gke-20260929/`.

After collecting evidence, the isolated cluster, replica/database disks, Gateway resources, reserved address, test buckets, test images, and dedicated service accounts were removed. A post-teardown audit confirmed no benchmark compute, disks, load-balancer components, network endpoint groups, buckets, or service accounts remained.
