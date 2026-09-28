# Python chat actor

From the repository root, build the runtime with `cargo build --locked`, install Node dependencies with `pnpm install`, and build the shared CLI with `pnpm --dir sdk build`.

In this directory:

```sh
uv sync
DURABLE_ACTORS_BINARY="$(pwd)/../../target/debug/durable-actors" DURABLE_ACTORS_ENTRYPOINT=actors.py node ../../sdk/dist/cli.js dev
```

In a second terminal, from this directory:

```sh
node ../../sdk/dist/cli.js generate
uv run client.py
```

The shared TypeScript CLI finds `.venv`, runs strict mypy, and generates Python clients. The generated `Chat` client accepts `generated.chat_models.Message` and returns a typed list of messages. Messages persist across runtime restarts. `Chat.connect(Member(name="Ada"))` opens a typed WebSocket; `Member` is also exported from `generated.chat_models`.

The example uses the repository SDK through `tool.uv.sources`. Remove that table when using the published package.
