# Pi agent

A basic Pi agent inside a Durable Actor, called through the separate [CLI](../cli/README.md). `prompt()` uses `@earendil-works/pi-agent-core` with OpenAI's `gpt-5-mini` and returns the completed text response. A `@Persisted messages` property saves chat history after each successful response and supplies it to the next prompt. History survives actor restarts; an interrupted model call is not resumed.

Requires Node.js 22.19+, Bun 1.3.9+, and pnpm. From the repository root:

```sh
pnpm install
pnpm --dir sdk build
cd examples/pi-agent
cp .env.example .env
pnpm dev
```

Set `OPENAI_API_KEY` in `.env` before starting the server (restart it after changing the key). Wait for `Ready` on port `7103`, then follow the CLI instructions in another terminal.

`pnpm check` checks the actor types.
