# Bank account (TypeScript)

The finished code from the [TypeScript guide](../../docs/reference/typescript-guide.md): a bank account actor with persisted, ephemeral and emitted fields, typed WebSockets, SQLite and sandbox overrides, plus an interleaved wire transfer actor.

[Guide](../../docs/reference/typescript-guide.md) · [Actors](src/actors.ts) · [Client](client.ts)

## Run locally

Requires Node.js 22.19+, pnpm, and Bun 1.3.9+. From the repository root, install dependencies and build the CLI:

```sh
pnpm install
pnpm --dir sdk build
cd examples/bank-typescript
cp .env.example .env
pnpm dev
```

The first run downloads the runtime. Wait for `Ready`, then in a second terminal from `examples/bank-typescript`:

```sh
pnpm generate
pnpm client
```

The client prints the `demo` account's balance before and after a $25 deposit. Run it again, or restart `pnpm dev` first, and the balance keeps growing because it persists.

The actor server listens on port 7111, so it can run beside the [Python bank example](../bank-python/README.md) on 7112.
