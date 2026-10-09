# Pi agent

A basic Pi agent inside a Durable Actor, called through the separate [CLI](../cli/README.md). `prompt()` uses `@earendil-works/pi-agent-core` with OpenAI's `gpt-5-mini` and returns the completed text response. Each call starts a fresh conversation; history and in-flight execution are not persisted.

Requires Node.js 22.19+, Bun 1.3.9+, and pnpm. From the repository root:

```sh
pnpm install
pnpm --dir sdk build
cd examples/pi-agent
cp .env.example .env
pnpm dev
```

Set `OPENAI_API_KEY` in `.env` before starting the server (restart it after changing the key). Wait for `Ready` on port `7103`, then follow the CLI instructions in another terminal.

`pnpm check` checks the actor types. `pnpm test` checks the Pi prompt and error handling with a fake model stream, then starts a temporary server, generates the sibling CLI's client, and verifies prompt validation without an API key. Tests make no paid model calls. To test with a locally built runtime, set `DURABLE_ACTORS_BINARY` to the absolute path of `target/release/durable-actors`.
