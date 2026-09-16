# Cross-region replication measurement

Measured September 16, 2026 UTC against the live QA installation. The complete
three-host replication path took **68.8 ms p50 / 74.2 ms p99** with the actor in
East and additional hosts in Central and West. The same-image zonal comparison
took **12.1 ms p50 / 21.0 ms p99** for that path: a 5.7× median difference.

GCS remained in the race. It won 872 of 1,001 cross-region commits (87.1%), so
actual snapshot persistence measured **59.5 ms p50 / 70.8 ms p99**. This is a
latency experiment, not a retained multi-region durability policy: replicas
still archive into the home bucket and can remove their local copies afterward.

## Results

All values are milliseconds. Each mode has 1,000 sequential client writes and
1,001 host observations, including the initial write. Every returned counter
value and final state matched expectations.

| Measurement | Zonal p50 | Cross-region p50 | Zonal p95 | Cross-region p95 | Zonal p99 | Cross-region p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Snapshot persistence, winning proof | 12.284 | 59.464 | 17.230 | 69.061 | 21.194 | 70.843 |
| Full replica path, including after GCS wins | 12.075 | 68.755 | 17.006 | 71.627 | 20.963 | 74.206 |
| GCS upload | 55.927 | 58.821 | 75.807 | 75.321 | 107.934 | 100.219 |
| Commit RPC | 36.536 | 36.974 | 49.238 | 54.806 | 60.792 | 68.901 |
| Client write | 119.905 | 162.961 | 133.051 | 188.731 | 144.829 | 207.532 |

The complete replica path requires the actor's local SQLite sync and both
remote acknowledgments. Its timer starts just before the persistence race;
snapshot persistence also includes a small amount of surrounding host work.
Both measurements exclude the subsequent fenced PostgreSQL commit. The client
receives success only after that commit.

| Cross-region component | Samples | p50 | p95 | p99 |
| --- | ---: | ---: | ---: | ---: |
| Primary local disk sync | 1,001 | 0.742 | 1.174 | 1.908 |
| Central replica acknowledgment | 1,001 | 35.604 | 36.118 | 36.606 |
| West replica acknowledgment | 1,001 | 68.598 | 71.452 | 74.065 |

Both modes recorded 1,001 successful replica sets, zero failed sets, 2,002
remote acknowledgments, and 1,001 successful GCS uploads. Replicas won 129
cross-region commits and all 1,001 zonal commits. West acknowledgment dominated
the cross-region path. The fastest observed full replica path was 66.754 ms
cross-region and 9.063 ms zonal; these are observed minima, not guarantees.

The configured race lets GCS provide the earlier proof while remote writes
continue. Requiring geographically distributed acknowledgments before commit
would wait for that full replica path; this experiment did not enforce that
stronger policy or exercise region loss.

## Method and limits

- Same preview control-plane image, actor fixture, SDK 0.1.38, cloud client,
  counter workload, and concurrency one. Cross-region ran first, then zonal.
- The client remained in AWS `us-east-1`. The cross-region actor also reported
  `us-east-1`; the zonal actor reported Azure `eastus2`. Actor hosts differed,
  so this is not a controlled single-host geography comparison.
- Remote replicas were verified by reading their own environment: GCP
  `us-central1` and `us-west1`. The zonal fleet used Azure `eastus2` and GCP
  `us-east4`, all under the existing coarse `north-america-east` policy.
  The name `zonal` does not attest a shared physical availability zone.
- Cross-region replicas were created from the new fixture. Zonal reused the
  existing 0.1.38 storage fleet from the earlier experiment; its storage HTTP
  and SQLite sync path were unchanged. Both measured actors used the new image.
- A separate 200-write cross-region warmup and 20-write zonal warmup were
  excluded. Central provisioning was delayed during the cross-region warmup:
  155 of its 201 commits had no replica attempt, and next-ticket provisioning
  added roughly one second to many commit RPCs. Every commit in the measured
  1,000-write run had a successful replica attempt. Cold-start behavior remains
  a separate issue; its raw observations are retained below.
- The QA test waits one second after its final read, outside timed samples,
  before collecting background completion logs. No replication or GCS
  completion was missing from either measured run.
- This measures small resident state and sequential writes, not concurrent
  throughput, large snapshots, host/zone/region loss, or metadata failover.
  Replica disks remain ephemeral. PostgreSQL and the home bucket remain part
  of the durability system. Archival does not preserve remote-region copies.

## QA support and final state

The QA suite now accepts `cross_region_preview`, records full replica-path
latency even after GCS wins, and groups acknowledgments by destination region.
The runtime records those regions in snapshot recovery manifests. Existing
manifests without region fields remain readable.

Both main QA services were restored to `zonal`, count `2`, explicit regions `[]`:

- Control plane: `little-actors-00010-qdg`
- Socket gateway: `little-actors-sockets-00009-2gl`
- Image tag: `cross-region-qa-277d8d5b3780`
- Image digest: `sha256:099b12d093a82300137cc5349df023b5eddb2f1346ea437aa1e89707cf66c62e`
- Actor fixture: `im-nx9nC3u2PCs85iXtKtJO5p`

The authenticated durability endpoint confirmed the restored policy. Temporary
Central and West replicas were terminated only after their SQLite archive queues
were verified empty. The temporary benchmark client and per-run actor deployments
were removed; the zonal replica fleet remains available.

The preview is built from the working tree, not a published npm release. The QA
repository's existing package pins were preserved. To reproduce on a cloud
client with the normal QA credentials and URLs, use SDK 0.1.38 and the fixture:

```sh
QA_ACTOR_IMAGE_ID=im-nx9nC3u2PCs85iXtKtJO5p \
QA_EXPECTED_DURABILITY=zonal QA_EXPECTED_REPLICA_COUNT=2 \
QA_DURABILITY_ITERATIONS=1000 npm run qa:performance -- durability
```

For cross-region, configure both services as described in the
[replication guide](../../guides/replication.md#cross-region-latency-experiment)
and set `QA_EXPECTED_DURABILITY=cross_region_preview`. Warm the fleet first and
verify replica-attempt counts; policy configuration alone does not prove that
the replica path was available. Restore zonal afterward.

Validation passed: 114 Rust tests across library, placement, transport, recovery,
and PostgreSQL suites; 59 offline QA tests; QA typecheck; Rust formatting; and
Clippy with four preexisting warnings.

## Evidence

- [Summary](summary.json)
- [Cross-region samples and filtered host events](cross-1000.json)
- [Zonal samples and filtered host events](zonal-1000.json)
- [Cross-region startup warmup](cross-warmup-200.json)
- [Zonal warmup](zonal-warmup-20.json)
- [Verified remote replica placement](replica-placement.json)
- [Build and restored deployment identity](build.json)
