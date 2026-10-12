# Persistence actor and CLI

[persistence-tests](persistence-tests/README.md) runs the actor that exercises immediate writes, atomic rollback, interleaving, and crash recovery. The separate [CLI](cli/README.md) calls it through a generated client. No model API key is needed.

From the repository root:

```sh
bun install
bun run --cwd sdk build
cd examples/persistence/persistence-tests
cp .env.example .env
bun run dev
```

Wait for `Ready`. In a second terminal, from the repository root:

```sh
cd examples/persistence/cli
cp .env.example .env
bun run generate
bun run start crash-read
```

Both projects use actor port `7103`. Follow the [CLI instructions](cli/README.md) for writes, crashes, and overlapping calls. If you already have `.env` files, keep your existing settings.

Run `bun run check` in each project after generating the CLI. From the repository root, `bun test --timeout 60000 examples/persistence/persistence-tests/tests` runs the local persistence integration tests.
