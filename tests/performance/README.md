# Embedded capture: cold write measurements

The embedded Rust adapter reduced median first durable write latency by 16–24% and
sampled peak process-tree RSS by 29–38% in these local runs.

| Actor / SQLite payload | Baseline write | Embedded write | Latency reduction | Baseline peak RSS | Embedded peak RSS |
| --- | ---: | ---: | ---: | ---: | ---: |
| Fresh / tiny | 189.0 ms | 155.6 ms | 17.6% | 125.8 MiB | 89.4 MiB |
| Restored / tiny | 209.5 ms | 161.0 ms | 23.2% | 143.9 MiB | 89.7 MiB |
| Fresh / 1 MiB | 197.1 ms | 165.7 ms | 16.0% | 126.7 MiB | 90.4 MiB |
| Restored / 1 MiB | 234.7 ms | 177.7 ms | 24.3% | 144.3 MiB | 92.5 MiB |

Each cell is a median of ten runs. RSS after the completed request also fell by
about 36 MiB. Idle control-plane RSS was approximately 13–14 MiB for both builds.
The sampled process count was four for fresh baseline actors, five during baseline
restore, and three for the embedded adapter.

## Method

- macOS 15.7.3, Apple M4, 16 GiB RAM; Rust 1.89.0 release profile, Bun 1.4.2,
  Node 22.19.0; upstream Litestream 0.5.17 for the baseline.
- Baseline: durable-actors `77bb819a5ab4e14a9d8ada217cb60ba0df6a32fa`.
  Candidate runtime and SDK: `2f9e3775` with terse-litestream
  `74bbfc3d26528746c58ea892e272e4ea0abd1abd`. SQLite capture uses the existing
  executor channel. Binary and SDK host SHA-256 fingerprints are in the data.
- Ten alternating baseline/candidate pairs at each payload size; each pair uses
  separate temporary projects on the same host machine. The baseline uses its
  matching protocol-23 SDK; the candidate uses the protocol-24 SDK. The public
  actor/client API and measured workload are identical.
- Start a fresh local control plane, wait for readiness, prepare the SDK client,
  then time its first `invoke`. This includes route discovery, actor host and Bun
  startup, capture setup or restore, execution and acknowledgement of the write.
  Control-plane startup and client module loading occur before timing.
- The first method creates a SQLite table, inserts a `zeroblob`, and increments a
  persisted counter. Restart the entire runtime, verify the blob length, then
  increment the counter again. Every restored run must return two. The blobs are
  highly compressible; these results do not characterize incompressible payloads.
- Sample RSS with `ps` across the control plane and all descendant processes from
  just before invocation through client completion. The Node client and sampler
  are excluded. Samples wait 5 ms between `ps` calls; actual intervals include
  command overhead. RSS sums can double-count shared pages and short peaks may
  be missed. These are sampled RSS values, not PSS or allocator statistics.
- No CPU quota, production network, GCS, or simulated contention. Filesystem and
  executable caches remain warm across trials. Compilation and integration tests
  were stopped before this batch. This is a local comparison, not a production
  cold-start or CPU-budget guarantee.

[Raw measurements](macos-arm64-executor-commit-2026-10-03.json) contain all 80 writes and summaries.
No failed or slow trials were dropped from that batch. Earlier harness smoke runs
are excluded.

## Reproduce

Initialize the vendored source with `git submodule update --init --recursive`.
Build the baseline and candidate in separate checkouts using the same release
profile, and preserve both executables. Build each revision's matching SDK,
install Bun and Node 22, and put the pinned upstream Litestream 0.5.17 binary on
PATH for the baseline. The protocol-version bump requires separate SDK builds.

```sh
python3 tests/performance/cold_write.py \
  --baseline /absolute/path/to/baseline \
  --candidate /absolute/path/to/candidate \
  --baseline-sdk /absolute/path/to/baseline/sdk \
  --sdk /absolute/path/to/candidate/sdk \
  --runs 10 --sizes 0 1048576 \
  --output /tmp/cold-write-results.json
```

Run without concurrent builds. The harness verifies persisted state across each
restart and writes results incrementally. It exits on failed writes, unexpected
state, or unsuccessful runtime shutdown. Repeat on the intended Linux CPU and
memory limits before choosing a production allocation.
