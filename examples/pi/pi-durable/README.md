# Pi Durable

The actor project for the [Pi example pair](../README.md). Each prompt to actor `demo` continues the same conversation; the separate [pi-cli](../pi-cli/README.md) sends prompts through a generated client.

## Run locally

Requires Bun 1.4.2+ and an OpenAI API key. From the repository root:

```sh
bun install
bun run --cwd sdk build
bun run --cwd packages/pi-durable build
cd examples/pi/pi-durable
cp .env.example .env
```

Set `OPENAI_API_KEY` in `.env`, then start the actor runtime:

```sh
bun run dev:actors
```

Wait for `Ready`. In another terminal, from the repository root:

```sh
cd examples/pi/pi-cli
cp .env.example .env
bun run generate
bun run start send "Remember that my name is Alice."
bun run start send "What is my name?"
```

Restart the runtime and ask again to check persistence. The example uses OpenAI's `gpt-5-mini`; change `model` in [PiAgent](src/actors.ts) to select another model. The runtime uses port `7104`.

## Storage

[PiAgent](src/actors.ts) creates `new DurablePiAgent(this, { models, registry, model })` and returns `result.text` from `agent.prompt()`. The [adapter](../../../packages/pi-durable/README.md) owns storage, opens and closes the harness per prompt, and restores Pi's transcript and task state from actor-local SQLite. Its private actor field is implicitly ephemeral; no decorator is needed.

For native Pi operations and integrations, use `agent.session.run(({ harness, conversation, context }) => ...)` inside an actor method. Adapter options also accept Pi's native harness options, and integration tools belong in the supplied registry.

This first version keeps the whole commit log in memory and replays it per call. Prompts run serially; this example has no tools or streaming. An interrupted prompt needs a subsequent call to reopen the harness and resume pending work.

## Checks

```sh
bun run test
bun run check
```

Tests exercise persistence using the real Pi harness without a model API key. Pi dependencies are pinned because its API is experimental. See the [upstream Pi Durable docs](https://github.com/earendil-works/pi/tree/main/packages/durable).
