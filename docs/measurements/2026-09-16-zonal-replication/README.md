# First zonal replication measurement

Measured September 16, 2026 UTC against the live QA installation. With two
additional storage hosts, median persistence fell from **59.7 ms to 11.4 ms**
(81%), and median client write latency fell from **172.7 ms to 126.8 ms** (27%).
This is an initial warm-write baseline at concurrency one.

## Results

All values are milliseconds. Client measurements contain 1,000 sequential writes
per configuration. Host measurements contain 1,001 commits, including one warmup.
Every returned counter value and the final state matched the expected count.

| Measurement | GCS only p50 | Two replicas p50 | GCS only p95 | Two replicas p95 | GCS only p99 | Two replicas p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Client write | 172.660 | 126.811 | 210.700 | 143.636 | 230.065 | 154.281 |
| Snapshot persistence | 59.718 | 11.388 | 78.994 | 15.157 | 103.847 | 18.166 |
| Commit RPC | 40.848 | 41.774 | 65.583 | 57.810 | 73.484 | 69.106 |

The fastest observed zonal persistence was **7.934 ms**; the fastest client write
was **85.229 ms**. These minima are observations, not guaranteed floors. Maximum
client latency was 382.462 ms for GCS only and 452.977 ms for zonal. The zonal
commit RPC had a 363.923 ms maximum, so occasional commit-path outliers remain.

Replication won all **1,001/1,001** zonal commits. Both remote acknowledgments
were recorded for every commit (2,002 acknowledgments), as was the primary's
local sync. The state authority still committed each snapshot in PostgreSQL.

| Zonal diagnostic | Samples | p50 | p95 | p99 |
| --- | ---: | ---: | ---: | ---: |
| Primary local disk sync | 1,001 | 2.580 | 5.047 | 6.347 |
| Individual replica acknowledgment | 2,002 | 10.145 | 13.524 | 16.332 |
| Background GCS upload | 1,001 | 57.395 | 73.776 | 97.791 |

All 1,001 background GCS uploads reported success before diagnostics were
collected. On the same zonal actor host, GCS upload remained around 57 ms at the
median while the replication proof completed around 11 ms. The aggregate archive
lag metric also includes independent archive-worker observations and therefore
has more samples than commits; it is not a count of unique archived snapshots.

The next substantial part of the warm-write path is the commit RPC, at roughly
42 ms median. That includes the metadata commit, ownership checks, network, and
preparation of the next write ticket; it is not isolated database execution time.

## Method and limits

- Same release image, SDK 0.1.38, fixture image, cloud client, counter workload,
  and concurrency for both modes. Object-storage-only was measured first.
- The client ran in one Modal sandbox reporting `new-york`. The control plane
  and database were in GCP `us-east4`.
- Actor placement used the same coarse east-coast policy. Modal placed the GCS
  baseline actor in `us-east-1` and the zonal actor in `us-ashburn-1`. These were
  separate actor hosts, so placement and timing can affect the end-to-end
  comparison. The concurrent zonal GCS-upload measurements provide an additional
  comparison from the same actor host.
- A 20-write GCS warmup and a 200-write zonal fleet warmup were excluded from the
  reported client samples. Each measured run also excluded its first actor write
  from client samples; host diagnostics include that write.
- This measures a small resident counter and sequential writes. It does not
  establish throughput under concurrent load, large-state performance,
  multi-zone failure tolerance, or regional/multi-region guarantees.
- Modal's coarse placement does not attest separate physical machines or zones.
  Replica disks remain ephemeral and subject to sandbox lifetime limits.
- QA fixtures capture runtime stdout in a local file so diagnostics can be read
  through the supported V2 filesystem/exec APIs before the actor is terminated.
  Both modes used that same instrumentation. The initial warmup exposed this
  collection issue and was rerun after the fix; it is excluded from these results.

## Deployment and reproduction

The main QA services were left in `zonal` mode with replica count `2`:

- Control plane: `little-actors-00008-l7j`
- Gateway: `little-actors-sockets-00007-tnr`
- Runtime image tag: `replication-qa-65ba3dfb8574`
- Image digest: `sha256:1eb38fa43d0a250d1110c0863bc6374a1e157e7ad830b600527c291c29bffd8f`
- Actor fixture: `im-5Y3XF98T09kWxYO20IzEN3`

The tag is a preview built from the working tree. It is not a new published npm
release. The QA repository's preexisting package/image pins were preserved;
its default image builder must be updated before it can build another replica
fixture automatically. Reuse the measured fixture in the same Modal workspace:

```sh
QA_ACTOR_IMAGE_ID=im-5Y3XF98T09kWxYO20IzEN3 \
QA_EXPECTED_DURABILITY=zonal QA_EXPECTED_REPLICA_COUNT=2 \
QA_DURABILITY_ITERATIONS=1000 npm run qa:performance -- durability
```

Run from a cloud client with the normal QA credentials and URLs. A workstation
run includes its own network latency. The original benchmark used a staged QA
checkout pinned to SDK 0.1.38 and the existing manual QA test.

For the GCS baseline, configure both services with
`DURABLE_OBJECT_DURABILITY=object_storage` and `DURABLE_OBJECT_REPLICA_COUNT=0`,
then run the same test with matching expected values. Restore zonal afterward.
Keep the same image and cloud client for comparisons.

## Evidence

- [Summary](summary.json)
- [GCS-only samples and filtered host events](baseline-1000.json)
- [Zonal samples and filtered host events](zonal-1000.json)
- [Build identity](build.json)

The QA log-collection regression test, all 58 offline QA tests, and the
TypeScript typecheck passed. Live actor deployments were removed after each run;
the shared storage replicas remain available for QA.
