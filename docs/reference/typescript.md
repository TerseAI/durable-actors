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
