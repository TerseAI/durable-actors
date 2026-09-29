# Rapid persistence across two zones

Measured September 28, 2026. This follow-up tests replacing passive sandbox storage replicas with parallel durable appends to independent Rapid buckets. It measures the storage primitive, not a complete actor invocation or a production replication protocol.

## Setup

Google GKE Sandbox (managed gVisor), GKE `1.35.8-gke.1225000`, one `e2-standard-2` sandbox node in each of `us-west4-a` and `us-west4-b`. Each probe Pod requests 250m CPU / 256 MiB and has a 1 CPU / 1 GiB limit. Both buckets were independently verified as `RAPID`, with zonal placement in A and B respectively.

Rust 1.89.0, `google-cloud-storage 1.17.0`, compile flag `google_cloud_unstable_storage_bidi`. Image digest: `sha256:1b099e7ec938ad26f92985684f3f5117daf0b6a4b2c46792f0b82e622a915856`.

One shared Rust storage client holds persistent append streams. Each write appends a versioned, checksummed frame and awaits `flush()`, checking its exact persisted offset. The two-bucket case starts both append-and-flush operations concurrently and acknowledges only after both finish. Payload construction/checksumming and log output are outside the measured interval. Client-visible SDK work and retries, if any, remain inside it.

For each origin zone and payload size, local-only, remote-only, and parallel two-zone writes alternate in rotating order. Each condition has 50 warmups and 500 measured writes per round. The first round ran A then B; an additional round ran B then A after the first showed origin-dependent variability. Both rounds are retained, for 18,000 measured writes and 1,800 warmups. The origins run sequentially. Payload sizes are experimental inputs, not product limits. Percentiles use the median and nearest-rank p95/p99. This is a short, single-actor, unloaded probe; it does not establish a production tail-latency SLO.

## Measurements

| Origin | Payload | Persistence | p50 ms | p95 ms | p99 ms | n |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| us-west4-a | 64 B | local | 3.152 | 7.291 | 11.762 | 1000 |
| us-west4-a | 64 B | remote | 3.274 | 7.783 | 11.703 | 1000 |
| us-west4-a | 64 B | both | 3.795 | 8.010 | 12.651 | 1000 |
| us-west4-a | 4 KiB | local | 3.834 | 7.139 | 9.873 | 1000 |
| us-west4-a | 4 KiB | remote | 4.022 | 6.950 | 9.864 | 1000 |
| us-west4-a | 4 KiB | both | 3.953 | 7.657 | 10.305 | 1000 |
| us-west4-a | 64 KiB | local | 4.113 | 5.979 | 9.087 | 1000 |
| us-west4-a | 64 KiB | remote | 3.426 | 5.526 | 8.951 | 1000 |
| us-west4-a | 64 KiB | both | 4.338 | 6.722 | 12.186 | 1000 |
| us-west4-b | 64 B | remote | 2.728 | 5.229 | 6.599 | 1000 |
| us-west4-b | 64 B | local | 2.097 | 3.008 | 3.707 | 1000 |
| us-west4-b | 64 B | both | 2.316 | 3.242 | 3.956 | 1000 |
| us-west4-b | 4 KiB | remote | 2.684 | 3.514 | 4.070 | 1000 |
| us-west4-b | 4 KiB | local | 2.522 | 4.484 | 13.662 | 1000 |
| us-west4-b | 4 KiB | both | 2.633 | 3.741 | 5.070 | 1000 |
| us-west4-b | 64 KiB | remote | 2.972 | 3.977 | 4.903 | 1000 |
| us-west4-b | 64 KiB | local | 2.861 | 3.933 | 4.625 | 1000 |
| us-west4-b | 64 KiB | both | 4.022 | 5.678 | 7.273 | 1000 |

The table above combines both rounds for each origin. Across both origins and both rounds:

| Payload | Persistence | p50 ms | p95 ms | p99 ms | n |
| ---: | --- | ---: | ---: | ---: | ---: |
| 64 B | local | 2.534 | 5.804 | 8.858 | 2000 |
| 64 B | remote | 3.060 | 6.812 | 9.382 | 2000 |
| 64 B | both | 2.958 | 6.612 | 10.550 | 2000 |
| 4096 B | local | 3.350 | 6.358 | 10.824 | 2000 |
| 4096 B | remote | 3.231 | 5.949 | 8.439 | 2000 |
| 4096 B | both | 3.244 | 6.443 | 10.292 | 2000 |
| 65536 B | local | 3.489 | 5.243 | 7.671 | 2000 |
| 65536 B | remote | 3.102 | 4.903 | 7.068 | 2000 |
| 65536 B | both | 4.200 | 6.072 | 9.699 | 2000 |

Paired incremental latency compares the two-zone write with the local-only write in the same iteration. These are adjacent operations, not simultaneous counterfactuals; scheduling and service variability still apply.

| Origin | Payload | Median added ms | p95 of paired difference ms |
| --- | ---: | ---: | ---: |
| us-west4-a | 64 B | 0.634 | 4.000 |
| us-west4-a | 4096 B | 0.073 | 2.549 |
| us-west4-a | 65536 B | 0.281 | 2.161 |
| us-west4-b | 64 B | 0.227 | 1.193 |
| us-west4-b | 4096 B | 0.151 | 1.589 |
| us-west4-b | 65536 B | 1.151 | 2.689 |

A closer look at each fanout write compares its total elapsed time with its own local append-and-flush duration. The difference includes waiting for the remote leg and fanout scheduling overhead. It is not a counterfactual estimate of local-only performance, since both legs were running concurrently.

| Payload | Median total minus local-leg ms | p95 difference ms |
| ---: | ---: | ---: |
| 64 B | 0.075 | 1.099 |
| 4096 B | 0.080 | 1.338 |
| 65536 B | 0.283 | 1.583 |

Separate streams/objects and backend variability can make a two-zone sample faster than a neighboring local-only sample. The observed small or negative differences should not be interpreted as replication accelerating writes. The stable conclusion is that this workload can wait for two durable zonal copies in low milliseconds.

## Integrity and writer termination

All 48 full-stream readbacks passed byte-for-byte sequence and checksum validation. Objects were read through the bidirectional API while still appendable, without object finalization. No measured write returned an application-visible failure.

A separate writer persisted versions 1–100 to both buckets, acknowledging each only after both flushes. It then persisted version 101 to bucket A only, explicitly did not acknowledge that write, and exited immediately with status 137 without Rust destructors or stream finalization. A new process in zone B used fresh read descriptors to recover each bucket independently.

| Bucket | Acknowledged versions recovered | Extra unacknowledged versions | Fresh-process read ms |
| --- | ---: | ---: | ---: |
| A | 100 | 1 | 138.372 |
| B | 100 | 0 | 47.276 |

The secondary contained every acknowledged write after writer termination. The extra version in A demonstrates why a production protocol must handle partial persistence and ambiguous outcomes. This probe does not implement or validate epoch fencing, membership changes, deduplication of customer requests, or selection of a recovery commit point. There was no actual zone outage, network partition, or second concurrent actor owner. The recovery durations are single observations, not latency percentiles. Bucket A was read first; the B read reused the client, so its duration is not a standalone cold-recovery measurement.

## Architectural implications

Cross-zone Rapid access works from the Rust SDK under GKE Sandbox. Parallel flushes provide the required physical copies before acknowledgement, without a storage replica process or its persistent disk. A production two-zone profile can use this mechanism to protect acknowledged data against one zone loss only when its ownership, recovery, code availability, and membership protocols preserve that guarantee. Native regional GCS availability and SLA do not automatically transfer to a pair of independent zonal buckets.

With both buckets required, one unavailable bucket stops new acknowledgements until redundancy is safely restored or the user explicitly selects weaker durability. Keep zonal acknowledgement as the configurable lowest-latency default. Keep GCS ownership separate from the hot persistence path.

Persistent connections avoid repeating stream setup for each write. Observed established storage connections used `142.251.2.207:443` and `74.125.137.207:443`, outside the documented direct-connectivity address range. This experiment does not verify ALTS/DirectPath; do not interpret the results as a measurement of Google’s advertised submillisecond direct-connectivity path. It also excludes gateway traffic, ownership acquisition, code activation, actor execution, and failover scheduling.

Google explicitly permits Rapid access from other zones and regions, with distance-dependent performance. Managed cross-bucket replication is not supported for Rapid, so the Rust runtime must perform the fanout: [Rapid documentation](https://docs.cloud.google.com/storage/docs/rapid/rapid-bucket). For the difference between zonal storage and native regional durability, see [Google’s durability documentation](https://docs.cloud.google.com/storage/docs/availability-durability).

## Reproduction and evidence

The isolated harness, pinned dependency lockfile, deployment/provisioning scripts, raw samples, bucket metadata, Pod/node inventory, build provenance, and analysis are retained in `.artifacts/rapid-multizone-20260928/`. Run `python3 .artifacts/rapid-multizone-20260928/analyze.py` to recompute the tables. The harness creates only temporary benchmark resources; it does not alter production routing or deployment. Cleanup status is recorded in `cleanup-audit.json`.

Cleanup audit at 2026-09-28T23:32:18.097733+00:00: the temporary cluster, nodes, disks, firewall rules, Rapid buckets, benchmark image, service account, and its added IAM bindings were all absent.
