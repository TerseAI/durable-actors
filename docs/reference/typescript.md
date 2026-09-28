# TypeScript documentation

Hover over SDK exports in your editor for types and API documentation.

To generate browsable TypeDoc documentation, run from the repository root:

```sh
pnpm install
pnpm docs:build
```

Open `.artifacts/api/index.html` in your browser. The documentation covers the SDK, backend helpers, proxy, and local runtime APIs.

Rerun `pnpm docs:build` after changing the SDK to refresh the documentation.

## SQLite and object fields

Each actor has a protected `this.db` handle for its own SQLite database. Keep using `@Persisted` for JSON fields and `@Ephemeral` for in-memory fields; `db` is provided by the runtime and must not be declared or decorated.

```ts
import { Actor, Persisted } from "durable-actors"

export class Notebook extends Actor {
    @Persisted edits = 0

    async add(text: string): Promise<number> {
        this.db.exec("CREATE TABLE IF NOT EXISTS notes (text TEXT NOT NULL)")
        this.db.exec("INSERT INTO notes VALUES (?)", text)
        return ++this.edits
    }

    async list(): Promise<{ text: string }[]> {
        return this.db.exec<{ text: string }>("SELECT text FROM notes ORDER BY rowid")
    }
}
```

`db.exec` synchronously prepares and executes one statement and returns an array of rows. Pass one statement per call. Bind values with `?` placeholders; bindings accept strings, numbers, bigints, byte arrays, and `null`. Queries returned through an actor method must still satisfy the usual JSON result contract.

After a successful method or socket hook, the runtime compares both the JSON fields and the SQLite database image. Changes to either, including SQLite table definitions and `PRAGMA user_version`, produce one durable snapshot containing both. Reads that leave both unchanged do not create another version. Existing actors with only JSON state acquire a database when they first use it.

Failed ordinary calls roll back SQL and object changes. Actors with `@Reentrant` methods retain the existing shared-state behavior: a successful overlapping call captures the current fields and SQL together, and a failed call cannot roll back another call's changes.

The runtime manages transactions and database files. Transaction control, attached databases, vacuuming, and storage-related pragmas are unavailable through `db.exec`. Database access is available only during the owning actor's invocation, after construction. SQLite tables are not automatically emitted over sockets.

Snapshots contain the JSON fields and full base64-encoded SQLite image, with no configured snapshot size cap. Memory use and commit cost grow with the combined state size. SQLite uses Bun's native driver in hosted actors and Node's built-in driver when running actors on Node.js 22.19+. Deploy matching SDK and Rust runtime versions together; the executor protocol rejects incompatible versions.
