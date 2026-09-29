# All-replica persistence benchmark

Measured September 29, 2026 on the local macOS machine using the implementation in this PR. This supersedes the Rapid experiments as a check of the selected storage path. It is not a GKE, zonal-network or cross-region measurement.

The harness starts real HTTP storage endpoints, each with a separate SQLite database (`journal_mode=WAL`, `synchronous=FULL`), and sends each update concurrently to every endpoint. It waits for all replies. One physical machine and disk serve all endpoints. The timed section includes client compression, HTTP transport, checksum verification, decompression and SQLite commit; it excludes actor execution, public routing, GCS ownership, pod startup and archival. The run used an unoptimized Rust test build with debug symbols and incremental compilation disabled.

Each case warms up for 10 writes and reports 100 serial writes. Payloads contain repeating letters plus a changing counter and request/version fields; they compress very well. Sizes below describe the payload field, excluding the snapshot envelope. Results are not representative of incompressible state or concurrent actor load.

| Replicas required | Payload | p50 | p95 | p99 |
| --- | --- | --- | --- | --- |
| 1 | 1 KiB | 0.261 ms | 0.317 ms | 0.426 ms |
| 3 | 1 KiB | 0.587 ms | 0.697 ms | 0.771 ms |
| 1 | 1 MiB | 8.639 ms | 9.531 ms | 9.598 ms |
| 3 | 1 MiB | 17.353 ms | 17.922 ms | 18.125 ms |

Reproduce from the repository root:

```sh
CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --locked --lib benchmark_all_replica_acknowledgements -- --ignored --nocapture
```

Small all-replica writes have sub-millisecond local overhead in this fixture. Large snapshots still pay for JSON snapshot handling, compression, checksum work, decompression and full-state disk updates; this is not a SQLite-WAL actor API. Production measurements should use the release image, realistic state entropy and sizes, sustained concurrency, and the selected Persistent Disk class.

For regional or multi-region placement, every write includes the slowest required replica's network round trip and disk commit. That cost cannot be inferred from loopback results. Before production cutover, measure from the actual client location with prewarmed GKE Sandbox pods, then separately exercise replica loss, zone loss, paused GCS uploads, replacement-disk recovery and cross-region routing. GCS archival runs after acknowledgement at a 16 MiB/10 second batching target; outages retain the pending records and delay archival.
