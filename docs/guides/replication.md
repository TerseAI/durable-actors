# Zonal replication performance preview

The first replication mode uses the existing actor host and additional Rust-only
Modal sandboxes. It keeps full immutable snapshots and the existing PostgreSQL
ownership and commit checks. Strict regional and multi-region policies are not enabled.

## Installation configuration

Set these on both the control plane and socket gateway:

```sh
DURABLE_OBJECT_DURABILITY=zonal
DURABLE_OBJECT_REPLICA_COUNT=2
```

The count is **additional storage hosts per active storage region**, shared across
namespaces and deployments. Two means the current actor host plus two storage
hosts. Counts from 1 through 8 are accepted. Replicas use the runtime binary in
the registered actor image, so that image must contain this runtime version.
They do not execute actor code or receive application secrets.

For an object-storage-only baseline, use `DURABLE_OBJECT_DURABILITY=object_storage`
and `DURABLE_OBJECT_REPLICA_COUNT=0`. This is the runtime default. An authenticated
`GET /v1/durability` reports the effective policy, count, and runtime version.

### Cross-region latency experiment

For an actor in `north-america-east`, the experimental placement is:

```sh
DURABLE_OBJECT_DURABILITY=cross_region_preview
DURABLE_OBJECT_REPLICA_COUNT=2
DURABLE_OBJECT_REPLICA_REGIONS='["north-america-central","north-america-west"]'
```

Destinations must be distinct and match the replica count. A home region that
appears in that list cannot use the replica path and falls back to GCS. The
durability endpoint reports the requested replica regions, and snapshot manifests
retain each replica's region alongside its recovery address.

This is a latency experiment with the same GCS race. A GCS win does not establish
synchronous cross-region durability. Replica writes continue in the background,
and `replica_set_persisted` reports completion of the full replica path even when
GCS wins. The measured `replication_ms` is separate from the winning persistence
time; per-replica acknowledgments also identify the destination region.

Archival still targets the actor's home bucket, and replicas can discard their
local copies after confirming that upload. This preview therefore does not promise
retained copies across regions after archival or regional metadata failover.
Use `zonal` with `DURABLE_OBJECT_REPLICA_REGIONS=[]` to restore the fast QA profile.

## Commit and recovery

For each snapshot, the actor host races:

1. GCS accepting the immutable snapshot.
2. Its local SQLite spool and **every configured replica** committing the bytes
   with SQLite WAL and `synchronous=FULL`.

Either proof allows the existing fenced PostgreSQL commit to proceed. The actor
returns success only after that commit. An unavailable replica can therefore
increase latency by making GCS win, without weakening the configured proof.
Network operations have bounded deadlines. Failure after actor execution retains
the existing `outcome_unknown` behavior; callers must not assume the write failed.

Replica locations are recorded in PostgreSQL before issuing the snapshot write
ticket. Hydration and inspection race GCS against those recorded locations,
including after replication is disabled or the current fleet has changed.
Uncommitted uploads never replace the authoritative state head.

The original GCS upload continues after replication wins. Every storage host also
retries archiving its pending snapshots, renewing GCS signed URLs through an
object-specific control-plane capability. A local copy is removed after upload;
when GCS reports that the object already exists, the archive worker checks the
bytes before removing its copy. Each spool accepts at most 1 GiB of pending
payloads; a full spool cannot contribute a replication acknowledgment.

## First-iteration limits

- Modal supplies coarse region placement. This adapter does not attest physical
  machine separation or availability-zone placement. Local tests verify recovery
  from surviving HTTP replicas after simulated host loss. The live QA benchmark
  measures latency; this preview does not establish a physical zone SLA.
- Storage sandboxes have no idle shutdown but retain Modal's 24-hour maximum
  lifetime. Their disks are ephemeral. GCS remains necessary for long-term
  retention; this version does not promise recovery from all replicas expiring
  during a prolonged GCS outage. Replacement nodes do not backfill historical
  pending snapshots; those remain on the original survivors until archived.
- PostgreSQL remains the commit authority and part of the latency and durability
  path. Protect and back it up together with GCS. Multi-region metadata failover
  is outside this iteration.
- Replica capabilities derive from the installation JWT signing key. Keep that
  key stable until pending snapshots have reached GCS. Different installations
  must use different keys. Archive capabilities last 72 hours.
- Replica sandboxes are shared and can outlive a QA deployment. A new runtime
  version gets a new fleet; old nodes continue archiving until they expire.

## Measurements

The [first live comparison](../measurements/2026-09-16-zonal-replication/README.md)
records 1,000 sequential writes per mode, raw samples, deployment identity, and
the limits of the comparison.

The [cross-region experiment](../measurements/2026-09-16-cross-region-replication/README.md)
records the complete East/Central/West replica path separately from the winning
GCS-or-replicas persistence time, alongside a fresh zonal comparison.

`actor_state_write` includes `snapshot_persisted_at_ms`, `durability_proof`, and
`commit_rpc_completed_at_ms`. `snapshot_uploaded_at_ms` is present only when the
bucket provided the proof. Additional events report local sync time, per-replica
acknowledgment time, the winning durability proof, GCS upload time, and archive lag.

Compare the same runtime and workload in object-storage-only and zonal modes.
Report sample counts and p50/p95/p99 for warm writes, persistence, and the commit
RPC, plus the fraction of writes for which replication wins. Commit RPC time
includes ownership checks and preparation of the next write ticket. Client timing
includes its network; host timing isolates the persistence path. Establish the
baseline before setting latency thresholds.


## Live failure QA

The companion QA repository provides `npm run qa:failure`. Use a dedicated
installation with its own signing key and replica fleet. Set
`DURABLE_OBJECT_MODAL_MUTABLE_NETWORK=true` on both of its services before
creating hosts. This opt-in starts Modal sandboxes with an explicit allow-all
network policy so QA can temporarily block GCS traffic. Normal installations
retain Modal's default open networking. Existing hosts are not reconfigured by
this setting; use a fresh fleet.

The test commits while GCS is unreachable from all three writers, verifies that
the snapshot is absent from GCS, terminates the actor and one replica, and checks
recovery through the remaining replica. It then restores network access and
verifies byte-for-byte archival. Details and evidence are in the
[live failure-recovery report](../measurements/2026-09-16-failure-recovery/README.md).
