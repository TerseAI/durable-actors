> Historical research: production now uses all-replica disk acknowledgements and batched Standard GCS archival. See [the deployment contract](../../charts/terse/README.md). These Rapid measurements are not replica latency measurements.

# Rapid persistence across two regions

Measured September 28, 2026. This experiment tests a single Rust process keeping append streams open to Rapid buckets in two regions and requiring durable flushes from both before acknowledging each write. It follows the [two-zone experiment](gcs-rapid-multizone-benchmark.md).

## Setup

The bucket pair is `us-west4-a` (Las Vegas) and `us-east4-a` (Northern Virginia). Each is a separate zonal Rapid bucket, not a native dual-region GCS bucket. Bucket location and storage class were read back from Google Cloud. These zones are listed in [Google’s supported Rapid locations](https://docs.cloud.google.com/storage/docs/locations#available-locations).

Each origin runs in its own temporary GKE Standard cluster with a Google-managed GKE Sandbox node (`runtimeClassName: gvisor`). The sandbox node is `e2-standard-2`; the probe requests 250m CPU and 256 MiB, with 1 CPU / 1 GiB limits. The trusted cluster default node runs system workloads. Rust 1.89.0 uses `google-cloud-storage 1.17.0` with `google_cloud_unstable_storage_bidi`. There is no filesystem mount or Go process in the measured path.

Image digest: `sha256:46c32546e57c26d537d11393c2be1472e7789ced9a53e14c6cb60afac8847889`.

For each origin and record size, the probe rotates among local-only, remote-only, and parallel two-region writes. Each case uses its own persistent append log, with 25 warmups followed by 250 measured writes. The two origins run sequentially, west then east. There are 4,500 measured writes and 450 warmups. Each flush must report the exact expected persisted offset. Records contain a sequence number and checksum; construction and logging are outside timing. The concurrent case awaits both append-and-flush futures.

Percentiles use the median and nearest-rank p95/p99. This is a short single-actor probe, without injected load or a full actor invocation. Sample record sizes are benchmark inputs, not product caps. Stream creation, ownership acquisition, code activation, and client/gateway latency are excluded from hot-write timing.

## Measurements

| Origin | Record | Persistence | p50 ms | p95 ms | p99 ms | n |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| us-west4-a | 64 B | local | 3.364 | 4.675 | 5.493 | 250 |
| us-west4-a | 64 B | remote | 70.891 | 71.914 | 72.771 | 250 |
| us-west4-a | 64 B | both | 79.546 | 81.265 | 90.154 | 250 |
| us-west4-a | 4 KiB | local | 3.119 | 4.655 | 6.473 | 250 |
| us-west4-a | 4 KiB | remote | 62.617 | 64.399 | 69.654 | 250 |
| us-west4-a | 4 KiB | both | 72.209 | 73.615 | 75.897 | 250 |
| us-west4-a | 64 KiB | local | 4.813 | 6.618 | 9.374 | 250 |
| us-west4-a | 64 KiB | remote | 82.555 | 86.163 | 92.115 | 250 |
| us-west4-a | 64 KiB | both | 96.974 | 101.293 | 101.983 | 250 |
| us-east4-a | 64 B | remote | 62.033 | 63.219 | 65.858 | 250 |
| us-east4-a | 64 B | local | 4.053 | 6.836 | 8.596 | 250 |
| us-east4-a | 64 B | both | 79.282 | 80.693 | 84.430 | 250 |
| us-east4-a | 4 KiB | remote | 72.248 | 73.675 | 75.307 | 250 |
| us-east4-a | 4 KiB | local | 3.664 | 4.800 | 6.831 | 250 |
| us-east4-a | 4 KiB | both | 62.802 | 64.234 | 66.454 | 250 |
| us-east4-a | 64 KiB | remote | 108.104 | 109.975 | 111.764 | 250 |
| us-east4-a | 64 KiB | local | 4.437 | 6.706 | 15.668 | 250 |
| us-east4-a | 64 KiB | both | 99.471 | 108.371 | 108.927 | 250 |

Paired differences compare the two-region write with its neighboring local-only write in the same iteration. These are separate streams/objects; service and scheduling variability remain.

| Origin | Record | Median added ms | p95 of paired difference ms |
| --- | ---: | ---: | ---: |
| us-west4-a | 64 B | 76.179 | 78.114 |
| us-west4-a | 4096 B | 69.129 | 70.504 |
| us-west4-a | 65536 B | 92.043 | 96.218 |
| us-east4-a | 64 B | 75.245 | 76.891 |
| us-east4-a | 4096 B | 59.133 | 60.607 |
| us-east4-a | 65536 B | 95.085 | 103.963 |

For the same two-region write, subtracting the slower append-and-flush leg from total elapsed time estimates fanout orchestration overhead. It includes differences in the start times of the two futures; it does not isolate a pure transport RTT.

| Origin | Record | Median total minus slower leg ms | p95 difference ms |
| --- | ---: | ---: | ---: |
| us-west4-a | 64 B | 0.359 | 0.958 |
| us-west4-a | 4096 B | 0.503 | 1.040 |
| us-west4-a | 65536 B | 0.585 | 1.341 |
| us-east4-a | 64 B | 0.009 | 0.060 |
| us-east4-a | 4096 B | 0.009 | 0.106 |
| us-east4-a | 65536 B | 0.009 | 0.055 |

## Recovery and partial writes

All 24 full-stream readbacks passed byte-for-byte sequence/checksum validation. All measured writes completed with the expected persisted offsets; no application-visible errors occurred.

A separate writer in Las Vegas synchronously persisted and acknowledged versions 1–100 in both buckets. It then persisted version 101 only in the Las Vegas bucket and did not acknowledge it. The writer exited immediately with status 137 without running Rust destructors or finalizing streams. The driver deleted the entire Las Vegas probe Pod and verified that it was absent before recovery.

A fresh process in Virginia opened only the Virginia bucket and recovered all acknowledged versions. It never read the Las Vegas bucket. A separate fresh process then inspected the Las Vegas log to confirm the deliberately partial write.

| Read | Acknowledged versions recovered | Additional unacknowledged versions | Read ms |
| --- | ---: | ---: | ---: |
| secondary | 100 | 0 | 179.726 |
| primary | 100 | 1 | 1950.361 |

These are single recovery observations, not percentiles. The experiment terminates primary compute and performs independent remote recovery; it does not simulate loss of an entire Google region, network partitions, stale concurrent owners, or ownership-service failure. It also does not implement a recovery commit-point protocol. The extra record in only one bucket demonstrates why partial persistence and ambiguous outcomes require explicit handling.

## Production decision

Use one Rust Rapid implementation for all production actor-state replication policies. Zonal acknowledgement remains the lowest-latency default. The regional profile requires flushes from the configured distinct zones, and the cross-region profile requires flushes from the configured regions. There is no fallback to sandbox storage replicas or Standard GCS state snapshots in the planned production architecture. GCS conditional ownership and immutable code delivery remain separate concerns.

Concurrent fanout avoids adding local and remote write times serially. It cannot remove the physical distance cost of synchronous cross-region acknowledgement. The latency of this Las Vegas–Virginia pair is not a prediction for every region pair. Asynchronous remote copies can preserve the local acknowledgement latency, but cannot claim zero loss of the latest acknowledged write after primary-region loss.

A storage copy in another region is not the same as a complete actor failover guarantee. Safe ownership acquisition, code availability, request deduplication, epoch fencing, recovery, and membership changes must also survive the promised failure mode. With all configured buckets required, losing one stops new acknowledgements until the required redundancy is safely restored.

Google supports Rapid access from other regions, but not managed cross-bucket replication. The application performs the parallel writes: [Rapid documentation](https://docs.cloud.google.com/storage/docs/rapid/rapid-bucket). This experiment uses the public Google storage endpoint through the Rust SDK and does not establish ALTS/DirectPath connectivity.

## Evidence and cleanup

Source, dependency lockfiles, build provenance, bucket placement, Pod/node inventories, raw measurements, network socket snapshots, and analysis are retained in `.artifacts/rapid-crossregion-20260928/`. Run `python3 .artifacts/rapid-crossregion-20260928/analyze.py` to regenerate the aggregates. All resource creation and deletion commands are logged. Cleanup audit status is in `cleanup-audit.json`.

Cleanup audit at 2026-09-28T23:49:00.983291+00:00: all experiment clusters, nodes, disks, firewalls, buckets, images, service account, and added IAM bindings were absent.
