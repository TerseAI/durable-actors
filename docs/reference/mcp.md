# MCP

The control plane serves an [MCP](https://modelcontextprotocol.io) endpoint so coding agents can inspect and call your actors. It exposes the [HTTP API](openapi.md) as tools and uses the same credentials.

```
<control-plane-url>/v1/projects/<project-id>/mcp
```

With `durable-actors dev`, connect Claude Code to the local project:

```sh
claude mcp add --transport http durable-actors http://127.0.0.1:7100/v1/projects/local/mcp
```

When `DURABLE_ACTORS_SECRET` is set on the server, send it as a bearer token:

```sh
claude mcp add --transport http durable-actors https://actors.example.com/v1/projects/<project-id>/mcp \
  --header "Authorization: Bearer $DURABLE_ACTORS_SECRET"
```

| Tool | Description |
| --- | --- |
| `describe_deployment` | Actor classes, their methods, and TypeScript declarations for every type |
| `list_actors` | Actor classes, then a page of instances with their status and open connections |
| `get_actor_state` | An instance's saved state, latest or by version, whole or at a JSON Pointer |
| `list_state_history` | An instance's saved state versions |
| `list_requests` | Request history with outcomes, timing, and errors |
| `list_logs` | Console output from actors |
| `get_health` | Request counts, success rate, p95 latency, and the longest queue waits |
| `list_websocket_sessions` | WebSocket sessions, each open, closed, or lost |
| `invoke_actor` | Call an actor method. Runs real code and can change state. |

List tools return pages: pass `nextCursor` back as `cursor` for the next one.
