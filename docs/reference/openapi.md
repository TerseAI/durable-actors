# OpenAPI

The [OpenAPI specification](openapi.yaml) describes the HTTP endpoints, authentication, request bodies, and responses.

Download the specification from a running server:

```sh
curl --fail http://127.0.0.1:7100/openapi.yaml -o openapi.yaml
```

Replace the origin with your hosted server's URL when needed. Import the file into an OpenAPI-compatible API client or documentation viewer.

The specification is public. When the server has `DURABLE_ACTORS_SECRET` set, API requests require `Authorization: Bearer <api-key>` with the same secret; keep it out of browser code. When the secret is unset, API requests need no authentication. A server listening beyond localhost warns when authentication is disabled but still starts.

## Local development

Run `durable-actors dev`. Local requests need no authentication unless you explicitly set `DURABLE_ACTORS_SECRET`; when set, send it as a Bearer credential. Backend clients use the local defaults without environment settings. Local development uses the same project-scoped endpoints as production. The default project ID is `local`; use your configured `DURABLE_ACTORS_PROJECT_ID` if you override it.

For example, request a WebSocket URL for `Room/lobby` in the default local project:

```sh
curl --fail http://127.0.0.1:7100/v1/projects/local/actors/Room/lobby/find-websocket \
  --header 'Content-Type: application/json' \
  --data '{"metadata": null}'
```

## Resolve and invoke

The SDK sends method calls to POST `/v1/projects/{project_id}/actors/{actor_name}/{actor_id}/invoke` at the control-plane origin. Each request includes `requestId`, `method`, `args`, and an optional `homeRegion`. Authenticate with your configured secret or a project session. The control plane resolves the owning host, forwards the call with a scoped invocation ticket, and returns the actor's result in the same response.

For example, invoke `Room/lobby.sendMessage` locally:

```sh
curl --fail http://127.0.0.1:7100/v1/projects/local/actors/Room/lobby/invoke \
  --header 'Content-Type: application/json' \
  --data '{"requestId":"request-1","method":"sendMessage","args":[{"text":"hello"}]}'
```

The control plane retries once only when the host explicitly rejects the call before execution or the connection cannot be established. The SDK does not replay failed requests. An `outcome_unknown` response means execution may have occurred; do not automatically retry it. `requestId` identifies an attempt and does not deduplicate execution.

The host's internal endpoint uses the same path but requires a scoped invocation ticket and `ownerEpoch`.
