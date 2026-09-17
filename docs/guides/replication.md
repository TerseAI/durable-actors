# Bucket authority and replica snapshots

The hosted runtime uses GCS conditional writes for actor ownership and host leases.
PostgreSQL stores administrative deployment data. Successful deployment changes
publish their runtime configuration to the coordination bucket, so routing,
hydration, lease renewal, state writes, and application credential issuance do not
query PostgreSQL. Startup opens PostgreSQL lazily when an administrative operation
needs it.

This follows celld's object-storage coordination approach using this project's
existing GCS backend and replica hosts. Actor state remains a full JSON snapshot.
Replica storage uses atomic blob files; no replica database or separate consensus
service is required.

## Configuration

Set these on the control plane and socket gateway:

```sh
DURABLE_OBJECT_COORDINATION_BUCKET=my-actor-state-bucket
DURABLE_OBJECT_STANDARD_BUCKETS='{"north-america-east":"my-actor-state-bucket"}'
DURABLE_OBJECT_DURABILITY=zonal
DURABLE_OBJECT_REPLICA_COUNT=2
```

The coordination bucket can also be a snapshot bucket. All instances must use the
same coordination bucket and signing key. Grant the control-plane identity object
read, list, create, and replace access. Keep control-plane clocks less than five
seconds apart. Hosts stop releasing results five seconds before their locally
confirmed lease deadline, measured from the start of the renewal request.

Use fresh runtime metadata and re-register deployments when adopting this layout.
Existing PostgreSQL actor placements are not migrated. Do not expire ownership,
lease, deployment, or referenced snapshot objects through a bucket lifecycle rule.

Replica counts from 1 through 8 are supported. Replicas are shared Rust-only Modal
sandboxes, built from the registered actor image. They do not execute actor code
or receive application secrets. Every replica in an actor's recorded set must
acknowledge a write for it to provide a replica durability proof.

For bucket-only operation, set `DURABLE_OBJECT_DURABILITY=object_storage` and
`DURABLE_OBJECT_REPLICA_COUNT=0`. For the existing cross-region preview, set:

```sh
DURABLE_OBJECT_DURABILITY=cross_region_preview
DURABLE_OBJECT_REPLICA_COUNT=2
DURABLE_OBJECT_REPLICA_REGIONS='["north-america-central","north-america-west"]'
```

Cross-region destinations must be distinct and exclude the actor's home region.
Archival still uses the home bucket; the bucket fallback does not promise
synchronous cross-region durability.

## Writes and takeover

Each ownership epoch records its host session, snapshot prefix, initial state,
and fixed replica set. A fresh epoch initializes that set before it can serve.
If provisioning fails, that epoch uses bucket-only writes.

The host races immutable bucket upload against its local blob and every recorded
replica. Replica acknowledgments require durable snapshot bytes and a durable
stream head. The winning proof completes the write locally, without a per-write
control-plane commit. Bucket writes verify ownership after uploading. A reusable,
short-lived epoch capability allows the host to derive subsequent snapshot names.
An ambiguous write retains the exact snapshot for retry without executing again.

Takeover first fences the old epoch with a conditional ownership write. It seals
a surviving old replica, recovers the latest full snapshot, and saves the recovered
state to the bucket before publishing the new active owner. Seals survive restart
and reject delayed appends. Full snapshots also cover earlier writes that used the
bucket fallback. Late uploads into an old prefix cannot change the new epoch's
starting state. An uncertain write may be included during recovery.

An epoch that used replicas requires a complete surviving replica witness during
recovery. If every witness is unavailable, recovery stops rather than guessing
that the newest archived snapshot includes every acknowledged write. Failed
recovery retains its predecessor and can be retried when a witness returns.

## Local storage and archival

Replica files contain a JSON metadata line followed by the original snapshot.
Writes sync a temporary file, publish it atomically, and sync the directory before
acknowledgment. Stream heads and seals use the same atomic replacement procedure.
The archive queue is rebuilt from blob files on restart. Each spool accepts at
most 1 GiB of pending snapshot payloads.

Archival uses an epoch-scoped capability to obtain upload access. It verifies an
existing object's bytes before deleting a local copy. Stream heads and seals
remain after archival. Keep the installation signing key stable while snapshots
remain pending. `DURABLE_OBJECT_REPLICA_DATA` selects the replica directory and
defaults to `/tmp/durable-object-replica`.

Modal storage remains ephemeral and has a maximum sandbox lifetime. This mode
does not promise recovery after every recorded replica disappears before recovery
can establish a complete witness. Physical machine or availability-zone separation
is not attested by the provider adapter. Runtime authority and retained snapshots
also depend on the configured buckets remaining available.
