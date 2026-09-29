# TypeScript documentation

Hover over SDK exports in your editor for types and API documentation.

To generate browsable TypeDoc documentation, run from the repository root:

```sh
pnpm install
pnpm docs:build
```

Open `.artifacts/api/index.html` in your browser. The documentation covers the SDK, backend helpers, proxy, and local runtime APIs.

Rerun `pnpm docs:build` after changing the SDK to refresh the documentation.

## SQLite

Each actor has its own database through protected `this.db.exec<Row>(sql, ...bindings)`. Calls synchronously execute one statement and return rows. Bind values with `?` placeholders; supported values are strings, numbers, bigints, byte arrays, and `null`. Actor method results must satisfy the JSON result contract.

SQL and `@Persisted` fields commit together after successful methods or socket hooks. Failed ordinary calls roll both back. Overlapping `@Reentrant` calls share state, and a failed call cannot roll back another call's changes.

Database access is available during actor invocations, after construction. The runtime owns transactions and database files; transaction control, attached databases, vacuuming, and storage-related pragmas are unavailable. SQLite on Node.js requires 22.19+. Deploy matching SDK and runtime versions.
