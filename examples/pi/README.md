# Pi actor and CLI

Two small projects: [pi-durable](pi-durable/README.md) runs the actor, and [pi-cli](pi-cli/README.md) sends prompts through a generated client. Actor `demo` keeps the same conversation across calls and restarts.

## Start the actor

Requires Bun 1.4.2+ and an OpenAI API key. From the repository root:

```sh
bun install
bun run --cwd sdk build
bun run --cwd packages/pi-durable build
cd examples/pi/pi-durable
cp .env.example .env
```

Set `OPENAI_API_KEY` in the actor's `.env`, then run:

```sh
bun run dev:actors
```

The actor runtime uses port `7104`.

## Send a prompt

Wait for `Ready`. In a second terminal, from the repository root:

```sh
cd examples/pi/pi-cli
cp .env.example .env
bun run generate
bun run chat "Remember that my name is Alice."
bun run chat "What is my name?"
```

Restart the actor runtime and ask again to check persistence. The CLI needs only the actor runtime URL; the model API key stays in the actor project. Regenerate the client after changing the actor API.

`bun run check` checks each project's types. In `pi-cli`, `bun run test` checks prompt handling and starts a temporary local test actor to exercise the CLI. In `pi-durable`, it runs the adapter's tests. These tests do not call OpenAI.
