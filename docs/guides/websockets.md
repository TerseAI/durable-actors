# WebSocket connections

Your backend authenticates the user, checks access to the requested actor, then calls the generated `actors.ChatRoom.prepareWebsocket({ actorId, metadata })` helper. Keep the server API key on the backend. Project routing and actor-bound tickets do not provide tenant-scoped issuance authorization; the admin key has installation-wide authority.

The helper returns a grant with `transport`, `homeRegion`, `websocketUrl`, `connectByMs`, and `authorizedUntilMs`. Pass the complete URL to a native browser socket, preserving all query parameters:

```js
const response = await fetch("/api/socket/ChatRoom/lobby", { method: "POST" })
if (!response.ok) throw new Error("Connection denied")
const { websocketUrl } = await response.json()
const socket = new WebSocket(websocketUrl)
socket.onopen = () => socket.send(JSON.stringify({ type: "post", text: "Hello" }))
socket.onmessage = event => console.log(JSON.parse(event.data))
```

See the [backend authorization example](../../sdk/README.md#browser-clients). Direct integrations use the connection and upgrade operations in [OpenAPI](../reference/openapi.yaml).

Open before `connectByMs`, normally within 60 seconds. Authorization defaults to 15 minutes; `authorizationLifetimeMs` accepts one second through one day, subject to issuer limits. The host closes the connection at `authorizedUntilMs`, including while idle or handling a message. Both deadlines are Unix timestamps in milliseconds. Treat the full URL as a credential and omit it from logs.

## Messages and state

Application JSON travels in text frames, with no special subprotocol or initialization/readiness frame. Messages sent after the browser's `open` event wait for `onConnect` to finish.

Public `@Persisted @Emittable` fields synchronize automatically:

```json
{"type":"state","state":{"count":0},"version":1}
{"type":"state_update","changes":{"count":1},"removed":[],"version":2}
```

Apply `changes` and delete fields listed in `removed`. Private, protected, and non-emittable fields are excluded. Actors without emittable fields send no automatic state frames. Explicit `socket.send()` and `this.broadcast()` messages share the connection; `state` and `state_update` are reserved types.

Output is not an acknowledgment of persistence. Reconnection, renewal, and replay of transient messages remain application responsibilities. See [TypeScript connections](../reference/api.md#connections-and-socket-output).

## Close behavior

| Code                | Meaning                                                      |
| ------------------- | ------------------------------------------------------------ |
| `1000`              | Normal closure.                                              |
| `1006`              | Observed abnormal disconnect; not a server-sent close frame. |
| `1011`              | Connection or actor socket handler failed.                   |
| `1012`              | Host stopping or ownership lost; obtain a fresh grant.       |
| `1013`              | Connection or output queue limit reached.                    |
| `4400`              | Invalid application message or actor handler failure.        |
| `4408`              | Authorization expired; obtain fresh authorization.           |
| Other `3000`–`4999` | Application close or rejection.                              |

Protocol and size failures may use other standard codes. A gateway restart requires a new connection; hibernation alone does not. Close the socket when its view is finished.

## WebSocket callbacks

Set `DURABLE_OBJECT_SOCKET_EVENT_URL` to receive successfully handled incoming-message events. The [OpenAPI webhook](../reference/openapi.yaml) defines the body and authentication. Delivery is asynchronous and best effort without retries; callback failure does not undo actor handling. Connection changes and outgoing broadcasts do not produce callbacks. Local development leaves this disabled by default.
