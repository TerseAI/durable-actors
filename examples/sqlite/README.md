# SQLite playground

A small actor project for trying the SQLite implementation in this checkout. Each `Notebook` actor has its own `notes` table and a JSON `@Persisted` edit count. Successful calls save both together; a failed call rolls both back.

## Run locally

Requires pnpm 10, Node.js 22.19+, Bun 1.3.9+, and Rust 1.89+. This project's `.npmrc` selects Node 22.19.0 for pnpm commands.

From the repository root, set up once:

```sh
pnpm install --frozen-lockfile
cd examples/sqlite
cp .env.example .env
pnpm run setup
```

`pnpm run setup` builds the SDK and Rust runtime from this checkout and generates the client. Rerun it after changing the SDK or Rust runtime. The `.env` points to that local binary so the runtime and SDK stay in sync.

Start the server:

```sh
pnpm dev
```

Wait for `Ready`. In another terminal, from `examples/sqlite`:

```sh
pnpm demo
pnpm notes add "My first SQLite note"
pnpm notes list
pnpm notes rollback
```

Each demo run adds one note and attempts one deliberately failed write. `rollback` leaves both the notes and edit count unchanged. Empty notes are rejected.

The runtime listens at `http://127.0.0.1:7103`, with project ID `sqlite-playground`. To use a different port, update both the port and control-plane URL in `.env`.

## Try persistence and isolation

Stop the server with Ctrl+C, run `pnpm dev` again, then run `pnpm notes list`. Your notes and edit count are still there. Saved state lives in this project's `.durable-actors/` directory.

Clients use actor ID `my-notebook` by default. A different ID gives you an independent database:

```sh
NOTEBOOK_ID=another-notebook pnpm notes list
NOTEBOOK_ID=another-notebook pnpm notes add "A separate notebook"
```

Run `pnpm observe` to open the existing observer UI for actor state, calls, and timings. Read SQL rows through `pnpm notes list`; the observer's object state shows the JSON edit count.

## Change the example

Edit `src/actors.ts` to try more SQL through `this.db.exec`. It takes one statement per call, with values bound through `?` placeholders. The runtime owns transactions and persistence. Table creation happens inside methods because database access is available during actor invocations.

`pnpm dev` watches actor source changes. After changing public methods, run `pnpm check` to regenerate `generated/` and type-check the actor. `src/client.mjs` demonstrates calls through the generated client.

## Verify

```sh
pnpm check
pnpm test
```

The integration test starts its own runtime on a free port with temporary state. It checks an empty notebook, parameter binding, empty-note validation, rollback, separate actor IDs, recovery after a full runtime restart, and writing after recovery. It can run alongside `pnpm dev` and does not change your playground data.
