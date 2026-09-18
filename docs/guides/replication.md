# Bucket authority and replica snapshots

GCS conditional writes govern actor ownership and host leases. Actor hosts claim ownership, recover state, renew leases and persist writes. PostgreSQL stores the installation's deployment and public contract. Local development uses the same ownership and recovery code with a file bucket.

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
  hosts/<encoded-host>/lease.json
  hosts/<encoded-host>/sessions/<encoded-session>
  snapshots/<shard>/<encoded-type>/<encoded-id>/<epoch>/<version>.json
```

Identity components use base64url without padding. The shard is the first byte of the actor key's SHA-256 hash; the epoch is 32 hexadecimal characters. Do not expire ownership, lease, session or referenced snapshot objects through bucket lifecycle rules. Local file-bucket objects live under `<data-dir>/objects/`.

## Writes and takeover

An ownership record identifies its host session, epoch and recovered starting snapshot. Replica membership is initialized once per host session. A replicated write waits for initialization; failure selects object storage for that session.

The host races an immutable bucket upload against its local blob and every recorded replica. Replica acknowledgments require durable snapshot bytes and a durable stream head. Either proof completes the write locally, followed by a lease check before releasing the result. Bucket writes verify ownership after uploading. Ambiguous writes retry the exact snapshot without executing actor code again.

Takeover waits for the previous host lease to end, marks its session as recovering and seals a surviving initialized replica. It recovers the latest snapshots into GCS, marks recovery complete and conditionally claims the actor with its recovered starting state. Seals survive restart and reject delayed appends. Late uploads into an old epoch cannot change the new epoch's starting state. Recovery may include an uncertain write.

A session that used replicas requires a complete surviving replica witness. If every witness is unavailable, recovery stops and can be retried when a witness returns.

## gRPC transport and archival

Snapshot reads and writes, replica initialization, stream-head reads, session sealing and archive preparation use authenticated gRPC over HTTP/2. Every internal ingress must support HTTP/2. Signed capabilities bind the operation, object or stream, region, replica identity and expiration. Credentials travel in gRPC metadata; capability addresses use `grpc://` or `grpcs://`. The [protobuf contract](../../proto/durable_object.proto) defines the services.

Replica files contain a JSON metadata line and the original snapshot. Writes sync a temporary file, publish it atomically and sync the directory before acknowledgment. Stream heads and seals use the same procedure. The archive queue is rebuilt from blob files after restart; each spool accepts at most 1 GiB of pending payloads.

Archival obtains upload access through `ArchiveService.Prepare` using an epoch-scoped capability, then writes through `SnapshotService.Write`. It verifies existing object bytes before deleting a local copy. Stream heads and seals remain after archival. Keep the signing key stable while snapshots are pending. `DURABLE_OBJECT_REPLICA_DATA` selects the replica directory and defaults to `/tmp/durable-object-replica`.

Modal replica storage is ephemeral and sandboxes have a maximum lifetime. Recovery is not guaranteed if every recorded replica disappears before a complete witness can be established. A bucket write that wins the durability race has the bucket's configured durability.
