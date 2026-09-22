# Run the examples together

Use Node.js 22.19+, Bun 1.4.2+, and the repository's pnpm workspace. The examples already link to `sdk`; use a native runtime built from the same repository revision. Set `DURABLE_ACTORS_BINARY` in each example's ignored `.env` to the absolute path of `target/debug/durable-actors` (or the matching release build). Leaving it blank uses the cached published runtime, which may differ from a development SDK.

Each example needs its own backend port, actor-server port, project ID, and shared secret. Copy its `.env.example` to `.env` if needed, then configure:

| Example                 | `PORT` | `DURABLE_ACTORS_PORT` | `DURABLE_ACTORS_CONTROL_PLANE_URL` |
| ----------------------- | ------ | --------------------- | ---------------------------------- |
| Chatroom                | 3001   | 7101                  | `http://127.0.0.1:7101`            |
| Collaborative documents | 3002   | 7102                  | `http://127.0.0.1:7102`            |
| AI chat                 | 3003   | 7103                  | `http://127.0.0.1:7103`            |

The local `.env` files are configured this way. AI chat additionally needs `OPENAI_API_KEY` in `examples/ai-chat/.env` to generate replies.

From the repository root, build all three:

```sh
pnpm --filter './examples/*' --parallel build
```

Then run the actor servers in one terminal:

```sh
pnpm --filter './examples/*' --parallel dev:actors
```

Run the application backends in another:

```sh
pnpm --filter './examples/*' --parallel dev
```

Open [Chatroom](http://127.0.0.1:3001), [Documents](http://127.0.0.1:3002), and [AI chat](http://127.0.0.1:3003). Each example stores actor data in its own `.durable-actors/` directory. Vite shares the corresponding backend's HTTP server, so live-reload connections do not compete for a separate shared port.

The configured-port regression tests run with `node --test tests/scripts/example-ports.test.mjs` after workspace dependencies and the SDK build are available.

With the services running, `node --test tests/examples/live.test.mjs` checks the pages, two-client chat broadcasts, concurrent document edits, reconnection, and AI chat history. It creates isolated `smoke-*` actors and retains their data. The model-reply check is skipped until `OPENAI_API_KEY` is configured; when enabled, it makes one small OpenAI request.

The current local setup is running in the background. Process IDs, URLs, and log locations are recorded in `.artifacts/local-examples/processes.json`. Stop the corresponding processes before launching replacements on the same ports. After setting the AI API key, restart its backend so it reloads `.env`.
