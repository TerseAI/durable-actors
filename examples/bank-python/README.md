# Bank account (Python)

The finished code from the [Python guide](../../docs/reference/python-guide.md): a bank account actor with persisted, ephemeral and emitted fields, typed WebSockets, SQLite and sandbox overrides, plus an interleaved wire transfer actor.

[Guide](../../docs/reference/python-guide.md) · [Reference](../../docs/reference/python.md) · [Actors](src/actors.py) · [Client](client.py)

## Run locally

Requires Node.js 22.19+, pnpm, Python 3.11+, and uv. The actors and client are Python; the shared Node CLI runs the dev server and generates clients. From the repository root, install dependencies and build the CLI:

```sh
pnpm install
pnpm --dir sdk build
cd examples/bank-python
uv sync
cp .env.example .env
pnpm dev
```

The first run downloads the runtime. Wait for `Ready`, then in a second terminal from `examples/bank-python`:

```sh
pnpm generate
pnpm client
```

The client prints the `demo` account's balance before and after a $25 deposit. Run it again, or restart `pnpm dev` first, and the balance keeps growing because it persists.

The actor server listens on port 7112, so it can run beside the [TypeScript bank example](../bank-typescript/README.md) on 7111. This example uses the repository SDK through `tool.uv.sources`; remove that table to use the published package.
