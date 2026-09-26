# OpenAPI

The [OpenAPI specification](openapi.yaml) describes the supported customer HTTP endpoints, authentication, request bodies, and responses. Hosted clients use the gateway origin. Only these documented customer operations are available through that gateway.

Download the specification from a running server:

```sh
curl --fail http://127.0.0.1:7100/openapi.yaml -o openapi.yaml
```

Replace the origin with your hosted server's URL when needed. Import the file into an OpenAPI-compatible API client or documentation viewer.

The specification is public. Hosted gateways require `Authorization: Bearer <api-key>` using the customer API key; keep it out of browser code. Local development permits unauthenticated requests when `DURABLE_ACTORS_SECRET` is unset.

## Local development

Run `durable-actors dev`. Local requests need no authentication unless you explicitly set `DURABLE_ACTORS_SECRET`; when set, send it as a Bearer credential. Backend clients use the local defaults without environment settings. Local development uses the same project-scoped endpoints as production. The default project ID is `local`; use your configured `DURABLE_ACTORS_PROJECT_ID` if you override it.

For example, request a WebSocket URL for `Room/lobby` in the default local project:

```sh
curl --fail http://127.0.0.1:7100/v1/projects/local/actors/Room/lobby/find-websocket \
  --header 'Content-Type: application/json' \
  --data '{"metadata": null}'
```
