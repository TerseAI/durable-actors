# Bucket authority and replica snapshots

GCS conditional writes govern each actor’s ownership and lease in one record. Actor hosts claim ownership, recover state, renew leases and persist writes. PostgreSQL stores the installation's deployment, public contract, spare claims, and replica lifecycle records. Local development uses the same ownership and recovery code with a file bucket.

## Configuration

Set these on each control plane:

```sh
DURABLE_OBJECT_BUCKET=my-actor-state-bucket
DURABLE_OBJECT_REPLICA_REGIONS='["north-america-central","north-america-west"]'
```

The list specifies replica placement and count: up to eight entries, including repeated regions. An empty list, the default, uses object storage only. A region does not guarantee separate physical machines or availability zones.

Control planes share the bucket and signing key. Give their identity object read, list, create and replace access. Actor hosts receive installation-scoped GCS credentials for ownership, leases and immutable snapshots. Credential refresh uses the control plane; lease renewal goes directly to GCS. Keep clocks less than five seconds apart. Hosts stop releasing results five seconds before their confirmed lease deadline, measured from the start of renewal.

## Object paths

```text
little-actors/v2/
  owners/<shard>/<encoded-type>/<encoded-id>.json
  hosts/<encoded-host>/sessions/<encoded-session>
  snapshots/<shard>/<encoded-type>/<encoded-id>/<epoch>/<version>.json
```

Identity components use base64url without padding. The shard is the first byte of the actor key's SHA-256 hash; the epoch is 32 hexadecimal characters. Do not expire ownership, session or referenced snapshot objects through bucket lifecycle rules. Local file-bucket objects live under `<data-dir>/objects/`.

## Writes and takeover

An ownership record identifies its host session, route, epoch, lease expiry and recovered starting snapshot. Lease renewal conditionally updates that same record.

For a new actor with a ready spare and no competing claim, the control plane reads ownership once and passes the absence result to the host. The host conditionally creates ownership and its lease together; readiness returns the epoch and lease so the control plane validates locally. This path uses one bucket read and one conditional write for ownership and leasing. Replica-session initialization still uses separate bucket operations, and pool claims still use PostgreSQL. Conflicts require rereading; resumed actors also require recovery.

Ownership records must contain their activation lease. Older layouts are unsupported.

Each actor activation gets its own replica sandboxes, claimed in parallel with primary startup. Generic replica spares already run the Rust storage listener, without actor state, Bun, or customer code. One assignment request binds each listener and its capabilities to an actor session; the listener checks the request's bearer token locally. Replica sandboxes are not shared between actors or activations.

A new activation writes directly to GCS while replicas are provisioned and initialized in the background. The control plane conditionally publishes the initial session membership in GCS and returns it to the primary. If a write is still pending when membership becomes ready, the primary sends that write's full snapshot to every replica and races their acknowledgments against the GCS upload. There is no initial-readiness cutoff. GCS can complete a write before replicas are ready; provisioning or initialization failures leave that path available while setup retries. Full snapshots include intervening writes that completed through GCS.

See the [cold-write sequence diagrams](cold-writes.md) for primary startup, replica assignment, and the first write's durability race.

The host races a direct immutable bucket upload against acknowledgments from every recorded replica. The actor host keeps no local snapshot files. Replica acknowledgments require durable snapshot bytes and a durable stream head. Either proof completes the write, followed by a local lease check before releasing the result. Successful bucket uploads need no ownership reread: takeover must wait for lease expiry, and snapshot paths isolate each ownership epoch. The other upload task continues while the actor host is running. Ambiguous writes retry the exact snapshot without executing actor code again.

Takeover waits for the previous host lease to end, marks its session as recovering and seals a surviving initialized replica. It recovers the latest snapshots into GCS, marks recovery complete and conditionally claims the actor with its recovered starting state. Seals survive restart and reject delayed appends. Late uploads into an old epoch cannot change the new epoch's starting state. Recovery may include an uncertain write.

A session that used replicas requires a complete surviving replica witness. If every witness is unavailable, recovery stops and can be retried when a witness returns.

## Repair and lifecycle

The primary reports failed replica writes and checks replica heads every ten seconds. The control plane claims replacement listeners from the replica spare pool, creating them on demand when the pool is empty; repeated failure reports reuse the same pending replacement. Writes continue using GCS if the complete recorded replica set cannot acknowledge. Replacing a sandbox does not lower the required acknowledgment count.

After provisioning, the primary briefly takes the write gate to drain any in-flight replicated write and select GCS-only writes. It releases the gate before replica initialization, snapshot seeding, and the conditional membership update. Catch-up also runs outside the gate: if another write committed while seeding, the host sends the newer full snapshot before checking again. A short local version check enables acknowledgments once replicas are caught up. An uncertain membership update leaves writes on GCS until reconciliation succeeds. Recovery fences concurrent replacement attempts through the session record.

The control plane persists replica identities and pending replacements in PostgreSQL so another controller can resume lifecycle work. Its cleanup loop runs every twenty seconds and leaves a two-minute grace period after provisioning activity. Superseded replicas remain until the recorded membership no longer references them. After the primary's lease ends, cleanup seals the session and checkpoints its recoverable state to GCS before retiring the replica sandboxes. Failed checkpointing or provider cleanup remains retryable. Replicas survive primary failure during this recovery window; they do not run customer code or take over as the executor.

The pool defaults to five idle spares per runtime image, region, resource configuration, and role (actor or replica). Replica pools use the configured replica regions; an idle target of zero creates listeners on demand. Idle expiry and pool resizing never retire assigned replica witnesses: the replica lifecycle controller retires them after safe recovery or membership replacement. The host also replaces replicas after twenty-two hours to rotate them before Modal's sandbox lifetime limit. Modal replica sandboxes have a 1 vCPU / 1 GiB memory limit. The replica lifecycle controller and its provider interface are independent of Modal.

## gRPC transport and replica storage

Snapshot reads and writes, replica initialization, stream-head reads and session sealing use authenticated gRPC over HTTP/2. Every internal ingress must support HTTP/2. Signed capabilities bind the operation, object or stream, region, replica identity and expiration. Credentials travel in gRPC metadata; capability addresses use `grpc://` or `grpcs://`. The [protobuf contract](../../proto/durable_object.proto) defines the services.

Replica files contain a JSON metadata line and the original snapshot. Writes sync a temporary file, publish it atomically and sync the directory before acknowledgment. Stream heads and seals use the same procedure. Each replica keeps the latest full snapshot per actor ownership stream, deleting superseded snapshots only after the new stream head is durable. Restart completes interrupted cleanup. The payload capacity is 1 GiB; a replacement needs room for both the previous snapshot and its replacement until publication completes.

Replicas do not upload snapshots to GCS or run archive workers. A surviving replica can supply committed state during recovery; the recovering host checkpoints that state to GCS before claiming the next ownership epoch. Stream heads and seals remain to fence delayed writes. `DURABLE_OBJECT_REPLICA_DATA` selects the replica directory and defaults to `/tmp/durable-object-replica`.

Modal replica storage is ephemeral and sandboxes have a maximum lifetime. Recovery is not guaranteed if every recorded replica disappears before a complete witness can be established. A bucket write that wins the durability race has the bucket's configured durability.
