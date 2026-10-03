# GKE WebSocket scalability — 2026-10-03

This run exercises one actor with up to 32,768 WebSocket connections. It separates a one-second-idle hibernation stress test from sustained fanout with a 60-second idle timeout. These are measured configurations, not a general capacity guarantee.

## Build and environment

- Runtime source: `398ca46`; benchmark/lifecycle test refinements: `bbcf651`. The runtime image contains the same production source as the final PR.
- Runtime image: `us-central1-docker.pkg.dev/fluid-analogy-473415-c2/public/durable-actors@sha256:86871913c651d49c9a43dbd8c00f3b8662f164c941e77b58d8da394dac5306b3`.
- GKE `terse-actors`, `us-west4-a`, Linux amd64. GVisor nodes: `e2-standard-8`; ordinary nodes: `e2-standard-4`.
- Isolated benchmark project, PostgreSQL instance, two gateway/control-plane replicas, and four Node 22.19 load generators. Production deployments were not changed.
- Actor sandbox: 2 CPU, 2 GiB memory, no prewarmed actors. Rust gateway: 1 CPU/1 GiB requested, 4 GiB memory limit, no CPU limit. Each gateway has an nginx TLS sidecar with two workers and a 2 GiB memory limit.
- Load travels from GKE pods through cluster-local TLS nginx to the Rust gateway and a gVisor actor sandbox. A client reaching the non-owning gateway is forwarded to the room owner. This measures that forwarding path too.
- Small JSON application messages; each broadcast increments durable actor state. Four shards verify delivery to every socket and reject duplicates. Broadcast latency includes coordinator HTTP calls and 10 ms polling.

## Results

The hibernation ramp passed from **14:53:38 to 15:02:46 UTC** (547.930 seconds). Admission is the time to add the next tier, including each socket's `onConnect` readiness; the first tier includes a cold start.

| Live sockets | Added sockets | Admission | Echo replies/s | Broadcast deliveries/s | Broadcast p95 | Worst round |
| -----------: | ------------: | --------: | -------------: | ---------------------: | ------------: | ----------: |
|          128 |           128 |  14.515 s |           33.1 |                  1,839 |         94 ms |      125 ms |
|        1,024 |           896 |   5.469 s |           34.4 |                 10,453 |        117 ms |      133 ms |
|        8,192 |         7,168 |  22.408 s |           35.2 |                 18,374 |        561 ms |      576 ms |
|       32,768 |        24,576 |  91.627 s |           18.8 |                 11,529 |      2,399 ms |   24,805 ms |

Echo uses 32 concurrent application senders for ten seconds and sums the per-shard reply rates. It measures application-handler round trips, not gateway automatic replies. Broadcast rates count recipient deliveries: 25 rounds at 32,768 sockets equals 819,200 deliveries, not 819,200 actor messages. The one-second idle timeout caused two sandboxes to serve the first 32,768-socket broadcast phase, including the 24.805-second outlier.

- Every tier passed exact maintained counts, on-demand tagged lists, retained metadata/tags, gateway automatic replies, changed sandbox hostname, and delivery checks. No unexpected socket failures or duplicate broadcasts were recorded.
- Kubernetes samples show no running or pending benchmark sandbox around automatic replies at 128, 8,192, and 32,768 sockets. At 1,024, the old pod was still finishing shutdown during the heartbeat sample; it exited successfully at 14:55:19, before the replacement started. The observer correspondingly reported that tier as live and the other three as dormant.
- Connection 32,769 closed with **1013**. Reconnecting **3,276 clients at concurrency 512** restored exactly 32,768 sockets; the slowest shard completed in **11.454 seconds**.
- All **819,200** post-reconnect deliveries passed, at **13,834 deliveries/s**, p95 **5,612 ms**, including further one-second-idle handoffs.
- A reader paused for **8 seconds** recovered all **64 × 256 KiB** messages in order (16 MiB).
- No gateway, TLS, generator, PostgreSQL, or actor-container restarts or OOM kills occurred during the completed ramp.

The sustained run passed from **15:04:55 to 15:07:05 UTC** (130.339 seconds). Opening all 32,768 sockets took **82.220 seconds**. Its 25 broadcasts delivered **819,200 messages in 38.930 seconds**: **21,043 recipient deliveries/s**, or **0.642 complete-room broadcasts/s**. Full-room latency was **1,529 ms p50**, **1,976 ms p95**, and **2,114 ms maximum**. All **387** background application messages received replies, the same sandbox served the entire measurement, and no socket failures were recorded.

### Sampled resources

| Component                                         | Hibernation ramp: peak CPU / working set | Sustained run: peak CPU / working set |
| ------------------------------------------------- | ---------------------------------------: | ------------------------------------: |
| Owning Rust gateway                               |                  1.225 cores / 1,080 MiB |                 1.132 cores / 832 MiB |
| Forwarding Rust gateway                           |                    0.464 cores / 561 MiB |                 0.982 cores / 511 MiB |
| TLS sidecar, largest sample across replicas       |                  0.957 cores / 1,030 MiB |                 1.037 cores / 966 MiB |
| Actor sandbox, largest sample across replacements |                    1.041 cores / 248 MiB |                 1.334 cores / 248 MiB |
| Load generator, largest sample across four pods   |                    0.862 cores / 686 MiB |                 0.814 cores / 669 MiB |
| Isolated PostgreSQL                               |                    0.128 cores / 342 MiB |                 0.135 cores / 358 MiB |

The gateway, TLS, database, load-generator, and actor containers had **zero restarts and no observed OOM kills** during both successful capacity runs. The resource collector produced 88 samples during the ramp and 21 during the sustained run, roughly six seconds apart including Kubernetes query time.

### Gateway recovery

After both load runs, the isolated room owner's PostgreSQL lease was deliberately expired. The owner returned HTTP **503** after **23.24 seconds**, Kubernetes removed readiness and restarted its control-plane container, and it registered a new gateway identity and recovered within **54.56 seconds**. A fresh WebSocket then connected and completed an application echo. This intentionally induced restart is separate from the zero-restart capacity runs; it does not demonstrate preservation of existing sockets across gateway failure.

[Machine-readable results and sampled resource summaries](gke-results-2026-10-03.json) retain the per-shard measurements, sandbox identities, container status, and health transitions.

## Interpretation and limits

The one-second idle timeout intentionally stresses handoffs. At 32,768 clients, fanout can delay the next application event long enough for the sandbox to hibernate, even with a sender scheduled every 100 ms. Those cold starts are included in the hibernation run's broadcast timings. The separate 60-second-idle run must stay in one sandbox throughout its broadcast phase.

The connection ceiling and busy-room delivery rate are different measurements. Pooled HTTP/2 removes a per-client actor-host connection, but one actor still executes its handlers and publishes durable state through the normal storage path. Fanout, TLS work, per-recipient copies, and gateway memory remain relevant at larger room and project counts.

Resource figures are the maximum sampled CPU and working-set memory from `kubectl top`, with a five-second pause between samples; they are not instantaneous peaks or RSS. Gateways were reused during the hibernation rerun, so allocator retention from earlier attempts is included. The sustained run rolls the gateways and starts fresh.

The public GKE ingress, long soaks, many busy actors, maximum-size message throughput, and indefinitely stalled consumers were not benchmarked. The bounded slow-reader check verifies eight seconds of stalled reading and ordered recovery of 16 MiB. Unbounded queues consume gateway memory and can exhaust it. A gateway restart disconnects its clients; only actor sandbox shutdown preserves them. The dedicated Helm gateway deployment is covered by chart tests; the measured replicas both served control-plane and gateway traffic.

Reproduce with [the benchmark instructions](README.md). The JSON evidence contains no credentials or grant URLs.
