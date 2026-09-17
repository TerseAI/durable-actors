# Bucket authority and replica snapshots

GCS conditional writes govern actor ownership and host leases. The actor host
claims ownership, recovers state, renews its lease, and persists writes directly.
PostgreSQL stores deployment configuration for the control plane. Resolving routes
and provisioning hosts can require PostgreSQL; calls to a cached actor route do not.
There is no second deployment record in GCS.

Actor state remains a full JSON snapshot. Replica storage uses atomic blob files.
Local development runs the same ownership and recovery code against a file bucket.

## Configuration

Set these on the control plane and socket gateway:

```sh
DURABLE_OBJECT_BUCKET=my-actor-state-bucket
DURABLE_OBJECT_REPLICA_REGIONS='["north-america-east","north-america-east"]'
```

One list specifies placement and count: up to eight entries, including repeated
regions. An empty list, the default, uses only object storage. To place two replicas
in different regions, use `["north-america-central","north-america-west"]`.
A region setting does not guarantee separate physical machines or availability zones.

All instances use the same bucket and signing key. Grant the control-plane identity
object read, list, create, and replace access. Hosts receive namespace-scoped GCS
credentials: mutable ownership and lease records, immutable snapshots. Credentials
refresh through the control plane; lease renewal goes directly to GCS.

Keep host and control-plane clocks less than five seconds apart. Hosts stop
releasing results five seconds before their locally confirmed lease deadline,
measured from the start of the renewal request.

## Object paths

All objects share this prefix:

```text
little-actors/v1/namespaces/<namespace>/
  owners/<shard>/<actor-type>/<actor-id>.json
  hosts/<host>/lease.json
  hosts/<host>/sessions/<session>.json
  snapshots/<shard>/<actor-type>/<actor-id>/<epoch>/<version>.json
```

Identity components use base64url without padding. The shard is the first byte
of the actor key's SHA-256 hash; the epoch is 32 hexadecimal characters.
Do not expire ownership, lease, session, or referenced snapshot objects through
bucket lifecycle rules. This change requires a fresh administrative database schema
and runtime metadata; it does not migrate previous SQL or bucket layouts.

## Writes and takeover

Each ownership record names a host session, epoch, and recovered starting snapshot.
Replica membership belongs to the host session and is initialized once for all its
actors. New actors can activate before this initialization finishes. A replicated
write waits for the session; failed initialization selects object storage for that
session.

The host races immutable bucket upload against its local blob and every recorded
replica. Replica acknowledgments require durable snapshot bytes and a durable
stream head. Either proof completes the write locally, followed by a lease check
before releasing the result. Bucket writes verify ownership after uploading.
The host derives subsequent snapshot names and reuses short-lived replica
capabilities. An ambiguous write retries the exact snapshot without executing again.

Takeover checks that the previous host lease has ended, marks its session as
recovering, and seals a surviving initialized replica. It recovers the session's
latest snapshots into GCS and marks recovery complete. The new host then claims
the actor with one conditional ownership write, recording its recovered starting
snapshot. Seals survive restart and reject delayed appends. Late uploads into an
old epoch cannot change the new epoch's starting state. An uncertain write may be
included during recovery.

A session that used replicas requires a complete surviving replica witness during
recovery. If every witness is unavailable, recovery stops rather than guessing
that the newest archived snapshot includes every acknowledged write. Recovery can
be retried when a witness returns.

## Local storage and archival

Replica files contain a JSON metadata line followed by the original snapshot.
Writes sync a temporary file, publish it atomically, and sync the directory before
acknowledgment. Stream heads and seals use the same replacement procedure.
The archive queue is rebuilt from blob files on restart. Each spool accepts at
most 1 GiB of pending snapshot payloads.

Archival obtains upload access through the control plane using an epoch-scoped
capability. It verifies an existing object's bytes before deleting a local copy.
Stream heads and seals remain after archival. Keep the installation signing key
stable while snapshots remain pending. `DURABLE_OBJECT_REPLICA_DATA` selects the
replica directory and defaults to `/tmp/durable-object-replica`.

Local development's file bucket uses atomic replacement and a shared file lock for
conditional writes. Its objects live under `<data-dir>/objects/` with the same path
layout as GCS. Launch configuration stays in memory; `runtime.json` only lets local
clients discover the address and API key. Explicit client environment variables
bypass that discovery file.

Modal replica storage is ephemeral and sandboxes have a maximum lifetime. Recovery
is not guaranteed after every recorded replica disappears before a complete witness
can be established. A bucket write that wins the durability race has the bucket's
configured durability, regardless of replica placement.
