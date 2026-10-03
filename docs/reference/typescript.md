# TypeScript reference

[Quickstart](../../sdk/README.md#quickstart) · [Runnable example](../../examples/chat/README.md)

Hover over SDK exports for API documentation, or build the full reference from the repository root:

```sh
pnpm install
pnpm docs:build
```

Open `.artifacts/api/index.html` for actor, client, backend, proxy, and local runtime APIs.

## SQLite

Each actor has its own database through protected `this.db.exec<Row>(sql, ...bindings)`. Calls synchronously execute one statement and return rows. Bind values with `?` placeholders; supported values are strings, numbers, bigints, byte arrays, and `null`. Actor method results must satisfy the JSON result contract.

SQL and `@Persisted` fields commit together after successful methods or socket hooks. Fields occupy JSON values in the reserved `__terse_fields` table; names beginning with `__terse_` or `_litestream_` are reserved. Failed ordinary calls roll both back. Overlapping `@Reentrant` calls share state, and a failed call cannot roll back another call's changes.

`this.db.execute(sql, ...bindings)` returns `{ rows, rowsWritten }`. It accepts a SQL script atomically, with bindings and results belonging only to the final statement; `rowsWritten` includes triggers and foreign-key actions and is zero for reads. `this.db.exec()` keeps its single-statement, row-array contract.

Use `this.db.transactionSync(() => { /* SQL operations */ })` for nested SQL savepoints. A thrown callback rolls back its SQL; releasing a savepoint does not commit the invocation. Callbacks must be synchronous. Savepoints do not roll back actor fields or external effects; ordinary invocation failure still rolls back SQL and persisted fields together.

Database access is available during actor invocations, after construction. The runtime owns transactions and database files; transaction control, attached databases, vacuuming, and storage-related pragmas are unavailable. SQLite on Node.js requires 22.19+. Deploy matching SDK and runtime versions.

## Commit-safe socket messages

Use `socket.sendAfterCommit(message)` or `this.broadcastAfterCommit(message, options)` for authoritative acknowledgments: the host releases them only after the invocation's state is durable and discards them if it fails. Ordinary sends still stream immediately. Post-commit sends are bounded to 512 queued effects and 24 MiB per invocation; they do not guarantee receipt or replay. For recovery, persist pending messages with stable event IDs, replay them on reconnect, deduplicate repeated events, and remove pending messages only after acknowledgment.

## Background tasks

`waitUntil` accepts a deferred callback: `this.waitUntil(async () => { await startSandbox(); this.status = "ready" })`. The request commits without waiting for callbacks, which begin after that commit. Each callback runs in a separate serialized invocation with actor database and socket access; its successful changes commit independently. A failed callback rolls back persisted changes; ephemeral state may reset on failure. Sibling callbacks continue with the restored fields. Callbacks registered by a failing invocation are discarded. Read actor fields and reacquire socket handles inside each callback.

Up to 64 callbacks may be pending. Pending work prevents idle eviction and participates in graceful shutdown, but is lost on a crash and is never retried. Callbacks still serialize with incoming requests: use short callbacks. Background tasks are unavailable on classes with reentrant methods.
