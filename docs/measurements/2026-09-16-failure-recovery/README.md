# Live replica recovery QA

## Experiment

A separate installation used zonal replication with two additional storage hosts,
its own signing key, and the existing QA PostgreSQL database and storage buckets.
Each test used a unique namespace. Main QA stayed on its existing zonal deployment.

The test:

1. Identified the two storage hosts from successful acknowledgments for the same
   snapshot, rather than assuming which shared sandboxes were in use.
2. Blocked GCS egress from the actor and both replicas, preserving their control
   plane, gateway, and peer routes.
3. Committed a counter write, checked matching bytes on both replicas, and proved
   that the exact snapshot object returned HTTP 404 from GCS.
4. Terminated the actor and one replica. The surviving replica supplied the
   committed snapshot to a replacement actor while GCS still returned 404.
5. Restored egress, verified byte-for-byte GCS archival and an empty survivor
   queue, then checked that the replacement accepted another write.

The [initial successful run](initial-recovery.json) recovered in **4,275 ms**.
The count and state version remained 3; the owner epoch advanced from 1 to 2.
Both terminated processes exited with code 137. Recovery used a different actor
host, and GCS returned 404 both before the loss and after recovery. After egress
was restored, GCS returned 200 with exactly the recorded bytes and the surviving
replica had zero pending snapshots.

The [final run](final-recovery.json), using the opt-in networking implementation,
passed the same assertions. Recovery took **5,690 ms**; the count and version
remained 3 and the epoch advanced from 1 to 2. The complete live test took
14.6 seconds. These are two individual recovery observations, not a percentile
estimate or a recovery-time guarantee.

Build identities are recorded in [initial-build.json](initial-build.json) and
[final-build.json](final-build.json). The final Cloud Build was
`a1fdc308-3a41-4eaa-9e74-e4fe8b114a96`. The actor fixture retained the same snapshot
runtime; the final image changed the control-plane/provider networking option.

This exercises real process loss and peer recovery. It does not simulate a
physical datacenter outage, loss of PostgreSQL, or simultaneous loss of every
unarchived copy. It also does not test rejection of a subsequent commit from a
live but stale owner.

## QA integration

The companion `little-durable-objects-prod` repository now contains:

- `qa/performance/durability.test.ts`: configurable iteration count and expected
  installation policy, with persistence, commit, full replica-set, per-region ACK,
  and winning-proof measurements.
- `qa/failure/replica-recovery.test.ts`: the opt-in live failure scenario above,
  including recovery and archival assertions and sanitized checkpoint artifacts.
- `npm run qa:failure` and a workflow suite choice. `qa:full` continues to run
  functional and performance checks; failure injection requires
  `QA_FAILURE_ISOLATED=true` on a dedicated installation.

The isolated services must enable `DURABLE_OBJECT_MODAL_MUTABLE_NETWORK=true`
before creating hosts. Modal requires an explicit initial network policy to
permit later changes. This option defaults off, preserving ordinary deployments'
network configuration. Existing hosts need a fresh fleet to use the option.

## Performance investigation

The [immediate post-failure benchmark](post-failure-performance.json) completed
200 client writes and recorded 201 server writes. Persistence p50 was 54.784 ms.
There were 162 successful replica-set attempts and 39 failed attempts; replicas
won 138 durability races and GCS won 63. A killed peer could remain in the fleet
cache for up to 30 seconds. This run is evidence of degraded operation after a
failure, not a new steady-state zonal performance floor.

A [controlled network probe](network-comparison.json) alternated wildcard-domain,
CIDR-only, and wildcard-domain policies on the same peer. Each phase measured
100 HTTP/2 requests after ten warmups, using a 512-byte POST to `/health`:

| Policy | p50 (ms) | p95 (ms) |
| --- | ---: | ---: |
| Wildcard, first | 0.575 | 0.728 |
| CIDR only | 0.551 | 0.670 |
| Wildcard, repeat | 0.547 | 0.804 |

The probe does not show a sustained median penalty from the wildcard policy.
The last phase did have two roughly 210 ms outliers. This small network-only
sample does not measure snapshot persistence or establish the cause of the
post-failure slowdown. A separate attempted steady-state benchmark was
interrupted and produced no usable result.

## Validation and cleanup

- 115 Rust tests passed, including the PostgreSQL replica-manifest persistence
  test with a real local database; seven existing tests remained ignored.
- The Go provider suite and 64 QA unit tests passed. QA typechecking and both
  repositories' whitespace checks passed.
- The final release image built successfully and the live recovery test passed.
- All seven experiment deployments were removed, including the interrupted
  benchmark. Surviving storage hosts were inspected for zero pending snapshots
  before termination; all eight recorded experiment replica sandboxes are stopped.
- The benchmark client, both temporary Cloud Run services, and their separate
  signing-key secret were removed. Archived experiment objects and measurement
  artifacts were retained.
- Main QA remained on `little-actors-00010-qdg` and
  `little-actors-sockets-00009-2gl`, both serving 100% of their traffic. Its shared
  replica fleet was not terminated.

## Operational follow-ups

1. Refresh the fleet promptly after transport failures. Coalesce concurrent
   refreshes and apply backoff so an outage does not trigger a provisioning storm.
   Expose fleet readiness and last successful ACK without putting provisioning
   on the write's critical path.
2. Bound background upload concurrency and expose pending bytes, oldest pending
   snapshot age, archive failures, and time remaining before host expiry.
3. Add manifest cleanup that protects the committed head and in-flight tickets.
   Deleting historical metadata must not remove a still-needed recovery address.

These remain follow-up work. Replicas still have ephemeral disks and a 24-hour
maximum lifetime; replacements do not backfill pending snapshots. GCS remains
the long-term retention path, and cross-region placement remains a latency
preview rather than a regional durability guarantee.
