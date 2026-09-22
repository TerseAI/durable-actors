# Reference

| Reference     | Where to find it                                                                                                                                   |
| ------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| TypeScript    | Editor hover help, or generated TypeDoc: run `pnpm install` and `pnpm docs:build` in a repository checkout, then open `.artifacts/api/index.html`. |
| HTTP          | [OpenAPI](reference/openapi.yaml), also served at your server's `/openapi.yaml`.                                                                   |
| CLI           | `npx little-actors <command> --help`.                                                                                                              |
| Configuration | [Environment variables](reference/configuration.md).                                                                                               |

For a runnable app, start with the [chat example](../examples/chat/README.md). Working on this repository? See [Contributing](../CONTRIBUTING.md).

Actor discovery uses two POST endpoints under `/v1/projects/{project_id}/actors/{actor_name}/{actor_id}`:

- `/find-actor` accepts `{}` or `{ "homeRegion": "north-america-west" }` and returns the owning host's route, RPC token, ownership epoch, home region, and expiry.
- `/find-websocket` accepts required `metadata` plus optional `homeRegion` and `authorizationLifetimeMs`, and returns an authorized WebSocket URL and connection deadlines. Authorize the application user before requesting a URL.

Both require a backend API key and return `Cache-Control: no-store`. Local development also exposes them under `/v1/actors/{actor_name}/{actor_id}` using the configured project ID.

This is a breaking HTTP API change: upgrade the runtime and SDK together. Direct HTTP callers must replace `/connect` with the appropriate endpoint and remove `transport` from request bodies. Responses and the SDK's `SocketGrant` type also omit `transport`.
