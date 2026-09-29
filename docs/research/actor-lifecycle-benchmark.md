# Prewarmed actor lifecycle benchmark

Measured September 29, 2026 against the all-replica/Standard GCS architecture in PR #107. These are **local component measurements**, not laptop-to-GKE results. Google Cloud rejected the saved credentials pending interactive reauthentication, and the earlier benchmark clusters had been deleted. No live GKE measurement was performed in this run.

The fixture uses the actual Rust assigned-host implementation, a prewarmed generic Bun process, the compiled SDK Counter actor, and three real HTTP replica servers. Each replica has a separate SQLite WAL database with `synchronous=FULL`; every write waits for all three. All processes and databases share this Mac. The authority and archive buckets use the filesystem implementation, and customer code is copied locally. Public HTTPS routing, SDK routing, the control plane, PostgreSQL, GCS requests, GCS code download, GKE/gVisor, and independent Persistent Disks are excluded. Control-plane telemetry has no available endpoint in this component fixture.

The workload increments or reads one persisted integer. The compiled artifact is 7,960 bytes. Requests are serial; this is not a throughput, large-state, or failure-domain benchmark. The Rust test build is unoptimized, running on macOS 15.7.3 with Bun 1.4.2.

The subsequent [live GKE benchmark](actor-lifecycle-gke-benchmark.md) measures the public SDK path with real GCS and independent replica disks.

## Case definitions

- **Cold write/read:** a different never-activated actor for each case. The generic Bun process is ready before timing begins, with no customer code installed. The timer includes local code installation, host initialization, ownership acquisition, actor hydration, and the first HTTP operation through receipt and validation of its result. Pod creation, generic runtime warmup, token issuance, and control-plane assignment transport are outside the timer.
- **Hot write/read:** HTTP operations against the resident actor after its first request. Each write increments persisted state and waits for all three replicas; the following read checks that value. The HTTP connection is reused.
- **Resume write/read:** separate returning actors, after the old host has exited through the idle path. The fixture checks that ownership is sealed and the lease released. It then waits for the real 10-second archive uploader to finish and verifies that every replica has released the old epoch's local payloads. A fresh prewarmed host must recover the correct value, have a different host ID, and acquire a higher epoch. The timer covers the same activation-plus-first-operation interval as the cold case, including recovery from the filesystem archive.

The fixture uses a 250 ms idle timeout to trigger suspension quickly. Idle waiting, archive waiting, and generic process prewarming are outside the request timers. The archival interval remains the production 10 seconds; no forced flush replaces it. Each run discards two activation warmups per case and retains 20 samples per cold/resume case, plus 200 hot writes and 200 hot reads. Incorrect results or invalid lifecycle transitions fail the entire run.

## Results

Pooled across two successful runs. [Raw samples](data/lifecycle-local-20260929.csv) and [run metadata](data/lifecycle-local-20260929.json).

| Case | Samples | p50 | p95 |
| --- | ---: | ---: | ---: |
| Cold write | 40 | 12.51 ms | 15.06 ms |
| Cold read | 40 | 12.50 ms | 14.86 ms |
| Hot write | 400 | 0.98 ms | 1.47 ms |
| Hot read | 400 | 0.45 ms | 0.54 ms |
| Resume write | 40 | 16.95 ms | 18.98 ms |
| Resume read | 40 | 13.05 ms | 14.19 ms |

Percentiles use the median for p50 and nearest rank for p95. Raw timings include activation-readiness and first-invocation durations for cold/resume cases. Components' medians need not sum to the total median. All measured samples are retained, including slower samples.

## Reproduction

Build the SDK with a supported Node version and have Bun available on `PATH`, then run from the repository root:

```sh
pnpm --dir sdk build
CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 \
  TERSE_LIFECYCLE_OUTPUT=/tmp/terse-lifecycle.json \
  cargo test --locked --lib benchmark_prewarmed_actor_lifecycle -- --ignored --nocapture
```

The harness lives in `tests/unit/host/lifecycle.rs`, with the replica fixture in `tests/support/replica_cluster.rs`. It uses temporary directories and loopback listeners; no production data or deployment is changed.

The [subsequent live run](actor-lifecycle-gke-benchmark.md) used an isolated GKE Sandbox deployment with prewarmed capacity, the release image, real GCS ownership/code/archive buckets, and replicas on distinct nodes using the selected Persistent Disk class. It measured the same six independent cases through the public SDK endpoint from the laptop and reported the internal activation interval separately. These local results do not establish production latency or regional/multi-region durability.
