# Observability

Run `npx little-actors observe` to browse actors, connections, and request history. The CLI opens a local UI and keeps the API key on its server. Use `--no-open` to print the URL instead.

## Actors and requests

Actor inventory shows live and dormant instances, connections, and waiting operations. Select an instance to see its activity.

Requests show method calls and WebSocket events. Duration includes processing and saving state; queue wait shows time before processing starts. Pause freezes the display while collection continues.

## History

Switch to **History** to filter by time, actor, or outcome. Use **Load older** for more records.

The admin API provides the same filters:

```http
GET /v1/observe/requests?actorName=ChatRoom&actorId=lobby&outcome=failed&limit=100
```

Results are newest first. Pass `nextCursor` with the same filters for another page. When `reset` is true, replace previous records. See [OpenAPI](../reference/openapi.yaml) for parameters and responses.

Local runtimes retain 10,000 events across restarts. Hosted history currently lasts only for the server process. Collection is best effort; the UI warns when events are lost or cannot be saved.

## Live updates

The UI reconnects automatically. Direct SSE clients resume request events using `resumeCursor` as `after` or `Last-Event-ID`; replace their window when `reset` is true.
