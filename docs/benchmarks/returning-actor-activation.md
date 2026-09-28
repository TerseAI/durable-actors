# Returning-actor activation benchmark

Run on September 28, 2026, on macOS arm64 with Rust 1.89.0, using the debug test profile. These are measured runs of the production activation code against file-backed storage and replicas with injected latency. They are **not live GCS measurements or end-to-end request timings**.

## Clean shutdown followed by immediate reactivation

Each storage GET, LIST, or CAS receives the indicated delay. Each replica RPC receives a 5 ms delay. Two replicas hold the same committed snapshot. There are 3 warmups and 30 measured samples per cell; percentiles use nearest rank.

| Storage delay | Version                             | Activation p50 | Activation p95 | Shutdown p50 |
| ------------- | ----------------------------------- | -------------: | -------------: | -----------: |
| 10 ms         | Original                            |      159.36 ms |      166.58 ms |     29.20 ms |
| 10 ms         | Read optimizations                  |      125.60 ms |      133.80 ms |     29.77 ms |
| 10 ms         | Read optimizations + clean shutdown |       42.96 ms |       44.95 ms |     61.42 ms |
| 25 ms         | Original                            |      316.09 ms |      325.30 ms |     61.14 ms |
| 25 ms         | Read optimizations                  |      235.75 ms |      239.78 ms |     61.34 ms |
| 25 ms         | Read optimizations + clean shutdown |       87.72 ms |       90.13 ms |    120.08 ms |
| 50 ms         | Original                            |      563.98 ms |      570.33 ms |    110.02 ms |
| 50 ms         | Read optimizations                  |      407.62 ms |      413.43 ms |    109.97 ms |
| 50 ms         | Read optimizations + clean shutdown |      162.45 ms |      164.17 ms |    220.32 ms |

At 25 ms per storage operation, median activation falls from 316.09 ms to 235.75 ms with the read optimizations, then to 87.72 ms with clean shutdown: **228.37 ms saved (72.2%)** versus the original.

| Version                             | GETs | LISTs | CAS writes | Total storage calls | Replica seal RPCs |
| ----------------------------------- | ---: | ----: | ---------: | ------------------: | ----------------: |
| Original                            |    6 |     1 |          3 |                  10 |                 2 |
| Read optimizations                  |    4 |     1 |          3 |                   8 |                 2 |
| Read optimizations + clean shutdown |    2 |     0 |          1 |                   3 |                 0 |

The final path performs the control-plane owner GET, the snapshot GET, and the new owner CAS. The owner hint carries the prior generation and the clean-shutdown marker; its base points to the final uploaded snapshot. This avoids the host owner GET, session recovery, and snapshot LIST. A stale hint still has to pass the owner CAS and falls back to a fresh read on conflict.

Clean shutdown adds a session GET/CAS before the existing owner release GET/CAS. The table reports that cost separately. The setup has already uploaded the final snapshot, so shutdown timings exclude waiting for a pending upload. In production, unregister also drains outstanding uploads, with a five-second bound for the clean-shutdown attempt. A failed or timed-out attempt releases the owner without a clean checkpoint and leaves activation on the recovery path.

## Crash with a replica-only commit

The final snapshot exists only on replicas, and the old owner lease expires without unregistering. This checks that the clean-shutdown optimization does not bypass recovery.

| Storage delay | Original p50 / p95 | Read optimizations p50 / p95 |    Final p50 / p95 |
| ------------- | -----------------: | ---------------------------: | -----------------: |
| 10 ms         | 187.92 / 197.77 ms |           154.08 / 160.86 ms | 150.26 / 155.74 ms |
| 25 ms         | 352.59 / 367.38 ms |           275.88 / 283.60 ms | 270.06 / 276.16 ms |
| 50 ms         | 629.81 / 638.50 ms |           475.16 / 482.00 ms | 471.41 / 479.48 ms |

Crash recovery uses 11 storage operations in the original version and 9 in both optimized versions, plus 2 seal RPCs and 1 replica read. The clean-shutdown change preserves this recovery path; small timing differences between the latter two versions are run-to-run variation.

## Method and reproduction

- Original: `661b432`; read optimizations: `e1ea0a6`; clean shutdown: `a6d235c`.
- The [benchmark harness](../../tests/unit/bucket/returning_benchmark.rs) runs the actual ownership CAS, recovery, session sealing, snapshot selection, and snapshot loading code. The original and intermediate commits use the same harness adapted to their existing registration API and `release_activation` shutdown path.
- The measured interval starts with the control-plane owner lookup and ends after the next host has recovered state and claimed ownership. Deployment lookup, sandbox provisioning, executor startup, and application code are excluded.
- For the clean-shutdown scenario, the prior snapshot is already present in both storage and replicas. The original and intermediate versions release the lease without sealing; the final version seals and checkpoints it. No background cleanup runs between shutdown and reactivation.
- File operations add their own overhead beyond the injected delay. Fixed delays do not model network jitter, GCS throttling, large snapshot transfer, or live infrastructure tail latency.
- Every sample checks the recovered bytes and the incremented owner epoch. Storage and replica operation counts are asserted constant across all samples in a case.
- All three benchmark executables were built before the reported runs. Runs were sequential with no concurrent compilation or test suite launched by this task.
- [Raw results](returning-actor-activation.csv) contain all 18 rows (540 measured activations, plus 54 warmups).

Run the current version from the repository root:

```sh
ACTIVATION_BENCH_LABEL=current ACTIVATION_BENCH_SAMPLES=30 \
  cargo test --locked --lib benchmark_returning_actor_activation -- --ignored --nocapture
```

Behavior tests separately verify that shutdown waits for a replica-acknowledged upload, failed uploads retain recovery, out-of-order uploads keep the newest head, empty and unchanged actors reuse their checkpoint, stale owner hints remain fenced, and concurrent recovery cannot hide a snapshot from activation.
