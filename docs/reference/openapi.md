# OpenAPI

The [OpenAPI specification](openapi.yaml) describes the HTTP endpoints, authentication, request bodies, and responses.

Download the specification from a running server:

```sh
curl --fail http://127.0.0.1:7100/openapi.yaml -o openapi.yaml
```

Replace the origin with your hosted server's URL when needed. Import the file into an OpenAPI-compatible API client or documentation viewer.

The specification is public. When the server has `DURABLE_ACTORS_SECRET` set, API requests require `Authorization: Bearer <api-key>` with the same secret; keep it out of browser code. When the secret is unset, API requests need no authentication. A server listening beyond localhost warns when authentication is disabled but still starts.

## Invocation retries

The SDK invokes the discovered host with one POST. It refreshes discovery and retries once only after a confirmed connection refusal, HTTP 401 before dispatch, or an explicit `not_executed` reply. These outcomes share one recovery attempt.

A `not_executed` reply includes a reason: `stale_owner`, `host_unavailable`, or `upstream_not_reached`. It guarantees that the invocation did not execute and cannot execute later from a queue. Actor-method errors use `failed` and never authorize automatic replay, even if their error code is `unavailable`.

Timeouts, lost responses, and generic gateway errors such as HTTP 502/503/504 can follow execution. The SDK reports `outcome_unknown` without replaying the invocation. This includes a provider tunnel closing without a response: it is not a confirmed connection refusal. Request IDs provide correlation, not durable deduplication.

## Local development

Run `durable-actors dev`. Local requests need no authentication unless you explicitly set `DURABLE_ACTORS_SECRET`; when set, send it as a Bearer credential. Backend clients use the local defaults without environment settings. Local development uses the same project-scoped endpoints as production. The default project ID is `local`; use your configured `DURABLE_ACTORS_PROJECT_ID` if you override it.

For example, request a WebSocket URL for `Room/lobby` in the default local project:

```sh
curl --fail http://127.0.0.1:7100/v1/projects/local/actors/Room/lobby/find-websocket \
  --header 'Content-Type: application/json' \
  --data '{"metadata": null}'
```
