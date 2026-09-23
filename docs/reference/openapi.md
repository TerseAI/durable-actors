# OpenAPI

The [OpenAPI specification](openapi.yaml) describes the HTTP endpoints, authentication, request bodies, and responses.

Download the specification from a running server:

```sh
curl --fail http://127.0.0.1:7100/openapi.yaml -o openapi.yaml
```

Replace the origin with your hosted server's URL when needed. Import the file into an OpenAPI-compatible API client or documentation viewer.

The specification is public. Authenticated endpoints require `Authorization: Bearer <api-key>`; use your backend's `DURABLE_ACTORS_SECRET` and keep it out of browser code.

## Local development

Run `durable-actors dev` and copy the printed connection settings into your backend's environment. Local development uses the same project-scoped endpoints as production. The default project ID is `local`; use your configured `DURABLE_ACTORS_PROJECT_ID` if you override it.

For example, request a WebSocket URL for `Room/lobby` in the default local project:

```sh
curl --fail http://127.0.0.1:7100/v1/projects/local/actors/Room/lobby/find-websocket \
  --header "Authorization: Bearer $DURABLE_ACTORS_SECRET" \
  --header 'Content-Type: application/json' \
  --data '{"metadata": null}'
```
