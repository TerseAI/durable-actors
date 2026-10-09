# CLI example

A separate client project connected to the [Pi-agent example](../pi-agent/README.md). It imports the generated client, with no imports from the actor source.

Set `OPENAI_API_KEY` in the Pi-agent project's `.env` and start its server first. Then, from the repository root in a second terminal:

```sh
cd examples/cli
cp .env.example .env
pnpm generate
pnpm start prompt "Explain Durable Actors in two sentences"
```

The CLI prints the completed model response. Prompts share the `demo` actor’s saved chat history.

`pnpm generate` fetches the running server's contract and writes the client to `generated/`. Regenerate after changing the actor API. Generated files are ignored by Git.

The `.env` points at `http://127.0.0.1:7103`. Change it to connect to a different actor server; Bun loads it when running the CLI. `pnpm check` checks the CLI types after generation.
