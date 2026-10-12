# Persistence tests

A Durable Actor for testing immediate writes, atomic rollback, interleaving, and crash recovery through the separate [CLI](../cli/README.md). No model API key is needed.

Requires Node.js 22.19+, Bun 1.3.9+, and pnpm. From the repository root:

```sh
pnpm install
pnpm --dir sdk build
cd examples/persistence-tests
cp .env.example .env
pnpm dev
```

Wait for `Ready` on port `7103`, then follow the CLI instructions in another terminal. `pnpm check` checks the actor types.

`testImmediate()` clears the persisted crash marker. `testAtomic(shouldFail)` clears the same marker inside an atomic method and optionally throws to test rollback.

`writeThenCrash(marker)` saves a persisted marker and immediately exits the actor worker with code 1. `getCrashMarker()` reads it after recovery. `atomicWriteThenCrash(marker)` performs the same write and exit under `@Atomic`; recovery must retain the previous marker because the transaction never commits.

`testInterleave(label, delayMs)` appends persisted start/end events around an asynchronous delay under `@Interleave`. `getEvents()` reads the event list. The CLI's `interleave-test` runs two overlapping calls against the same actor; `events-read` checks the events after recovery.

The actor is named `PersistenceTests`. Its state is separate from the former `PiAgent` actor; renaming the example does not migrate or delete Pi data.

## Local control plane with staging GKE and Rapid

From the repository root, configure `.env.staging` using `.env.staging.example`, then run:

```sh
pnpm staging:persistence
```

Replace the template's example names and absolute paths with your staging resources. `STAGING_PROBE_NAMESPACE` and `STAGING_PROBE_DEPLOYMENT` identify an existing staging deployment whose ready pod is used to verify connectivity before setup. Cluster, namespace, bucket, and registry repository names must include a `staging` or `test` segment and must not include a `prod` or `production` segment. These name checks help catch configuration mistakes; they do not restrict the permissions of your Google credentials.

`staging:persistence` calls `node scripts/staging.mjs persistence-tests`. The argument selects a folder under `examples/` with a `src/actors.ts` entrypoint; the same script can launch other TypeScript examples. `STAGING_SECRET_KEYS` lists the comma-separated keys from `.env.staging` to pass to the actor pods; leave it empty for examples without secrets.

This staging-only launcher requires Docker, gcloud, kubectl, psql, Rust, Node, Bun, pnpm, and ngrok. Google application credentials, a local Postgres database, and Tailscale access to staging pods must already work. Set `STAGING_SECRET_KEYS=` and all application settings in the root `.env.staging`. This example needs no actor secrets. The launcher does not read the normal `.env`, the example's `.env`, or application variables exported in your shell.

The launcher creates or reuses the dedicated staging test namespace, private image repository, and a gVisor node pool (zero to one `e2-standard-2` node). It builds the current runtime for Linux, publishes the compiled persistence-test actor bundle, starts a local control plane through ngrok, and registers the example with the staging Rapid buckets. The first build can take a while; subsequent builds use Docker's cache. It never changes the production cluster.

The launcher runs `durable-actors start` using the locally built runtime and prints the control-plane URL when ready. Connect your existing CLI to that URL with the project ID and secret from `.env.staging`, then generate its client and run the test commands there.

Ctrl+C stops the local tunnel and control plane. Cloud resources and saved actor data remain, and the staging node pool can continue to incur charges until it scales down or is removed.
