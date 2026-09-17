# HTTP and WebSocket reference

This page documents deployment management, backend access, WebSocket connections, and application callbacks. Use the [TypeScript API](api.md) for actor methods and the [self-hosting guide](../guides/self-hosting.md) to configure a server.

- [Authentication](#authentication)
- [Deployments](#deployments)
- [Object inspection](#object-inspection)
- [Storage regions](#storage-regions)
- [Public signing keys](#public-signing-keys)
- [Direct WebSocket connections](#direct-websocket-connections)
- [WebSocket callbacks](#websocket-callbacks)
- [Advanced scopes](#advanced-scopes)
- [Session tokens](#session-tokens)
- [HTTP errors](#http-errors)

## Authentication

Use your configured server origin as the base URL. JSON requests use `Content-Type: application/json`. Backend operations require:

```http
Authorization: Bearer <api-key>
```

Use the server's `DURABLE_OBJECT_API_KEY` on your trusted backend to manage deployments, call actors, and publish updates. Browser apps use the generated SDK and your authenticated proxy endpoint to obtain actor-scoped WebSocket tickets.

| Operation                      | Method and path                                       | Credential                                    |
| ------------------------------ | ----------------------------------------------------- | --------------------------------------------- |
| Register or replace deployment | `PUT /v1/deployment`                                  | API key.                                      |
| Read deployment                | `GET /v1/deployment`                                  | API key.                                      |
| Remove deployment              | `DELETE /v1/deployment`                               | API key.                                      |
| List saved objects             | `GET /v1/objects`                                    | API key only.                                 |
| Inspect committed state        | `GET /v1/actors/{actorType}/{actorId}/state`          | API key only.                                 |
| Read public signing keys       | `GET /.well-known/jwks.json`                          | None.                                         |
| Connect from a backend         | `GET /v1/actors/{actorType}/{actorId}/websocket`      | API key; WebSocket upgrade.                   |
| Issue a socket ticket          | `POST /v1/actors/{actorType}/{actorId}/socket-ticket` | API key only.                                 |
| Connect from an app            | `GET /v1/socket`                                      | Ticket in the first frame; WebSocket upgrade. |

Call actor methods and send application broadcasts through the [TypeScript SDK](api.md). Management JSON request bodies are limited to 16 MiB; larger bodies receive `413`.

Path parameters `actorType` and `actorId` follow the [actor identity limits](api.md#identity).

## Deployments

All three operations require the API key and manage the default deployment.

### PUT /v1/deployment

Registers actor code for your application. There is one active deployment. The JSON request replaces the complete deployment specification.

```json
{
    "codeRevision": "chat-v1",
    "imageRef": "im-your-actor-image",
    "workingDirectory": "/workspace",
    "actorEntrypoint": "dist/actors.mjs",
    "secretRefs": [],
    "socketGatewayUrl": null,
    "warmRegion": "north-america-east"
}
```

**JSON parameters**

- `codeRevision` (`string`, required) — Revision label, 1–128 ASCII letters, digits, `.`, `_`, or `-`. Use a new label for changed code.
- `imageRef` (`string`, required) — Provider image reference containing the actor project, 1–255 bytes. Registration does not upload or build the image.
- `workingDirectory` (`string`, required) — Absolute project path inside the image, at most 1024 bytes.
- `actorEntrypoint` (`string | null`, default `null`) — Actor artifact produced by `little-actors build`, 1–1024 bytes when supplied. Relative paths resolve from the working directory. When omitted, the server uses `dist/actors.mjs`. The build checks actor definitions and [field annotations](api.md#saved-state-and-serialization). An explicit TypeScript path uses source loading for development.
- `secretRefs` (`string[]`, default `[]`) — Up to 16 provider secret names. Each contains 1–255 ASCII letters, digits, `.`, `_`, or `-`.
- `socketGatewayUrl` (`string | null`, default `null`) — Separate HTTP(S) origin for socket delivery. No path beyond `/`, credentials, query, or fragment. Configure clients' gateway origin to match.
- `warmRegion` (`string | null`, default `null`) — Configured storage region in which to request background image warmup. It is not retained in the deployment record.

**Response:** `200 OK` with JSON:

```json
{ "changed": true }
```

An identical deployment returns `{"changed":false}`. Changing the specification stops its previous cloud hosts before registering the replacement. Saved actor state remains, so new code must support existing state. This is not a zero-downtime rollout guarantee.

**Errors:** `400` for an invalid specification and `401` for a rejected admin credential. See [HTTP errors](#http-errors) for shared failure responses.

Warmup is asynchronous and does not guarantee an already running actor. Invalid or unconfigured warmup regions are skipped; warmup failures are logged without turning a successful registration into a failed response.

### GET /v1/deployment

Reads the active deployment.

**Response:** `200 OK` with the stored specification, including its internal application identity:

```json
{
    "namespaceId": "default",
    "codeRevision": "chat-v1",
    "imageRef": "im-your-actor-image",
    "workingDirectory": "/workspace",
    "actorEntrypoint": "dist/actors.mjs",
    "secretRefs": [],
    "socketGatewayUrl": null
}
```

If no deployment exists, the response is JSON `null`. The record omits `warmRegion`.

**Errors:** `401` for a rejected admin credential; `500` if the deployment cannot be read.

### DELETE /v1/deployment

Stops the deployment's cloud hosts and removes the active deployment registration.

**Response:** `200 OK` with `{"changed":true}`, or `{"changed":false}` if no deployment existed. It preserves saved actor state. Register actor code again before making new calls.

**Errors:** `401` for a rejected admin credential; `500` if removal fails.

There is no public actor-state deletion or individual actor reset API. Expose application-specific reset behavior as an actor method if needed.

## Object inspection

These read-only endpoints require the admin API key. Session tokens and socket tickets are rejected. Successful responses use `Cache-Control: no-store`. The [CLI](cli.md#inspect-saved-objects) uses the same endpoints for local and cloud runtimes.

### GET /v1/objects

Lists objects with committed state across all namespaces, including stopped objects and objects without a current deployment. Objects with no committed state are omitted.

Optional query parameters:

- `namespace` — Exact namespace filter. Omitting it lists all namespaces.
- `limit` — Page size from 1 to 500; defaults to 100.
- `after` — The previous response's `nextCursor`, URL-encoded.

**Response:** `200 OK` with JSON:

```json
{
    "objects": [
        {
            "namespaceId": "default",
            "actorType": "ChatRoom",
            "actorId": "lobby",
            "objectId": "object.v1.default.ChatRoom.lobby",
            "homeRegion": "north-america-east",
            "stateVersion": 3,
            "stateObject": "snapshots/01/0123456789abcdef0123456789abcdef/default/ChatRoom/lobby/3.json",
            "lastRequestId": "request-3"
        }
    ],
    "nextCursor": null
}
```

Results are ordered by storage identity. `nextCursor` is `null` on the last page; otherwise repeat the request with that cursor and the same filter. Each page reads current database records, so the list is not a single snapshot across concurrent commits.

### GET /v1/actors/{actorType}/{actorId}/state

Reads the default namespace. To select a namespace, use:

```http
GET /v1/namespaces/{namespaceId}/actors/{actorType}/{actorId}/state
```

**Response:** `200 OK` with the same object fields as listing, plus `state`, a JSON object containing all committed persisted fields, including internal fields. An existing placement with no committed snapshot has `stateVersion: 0`, `stateObject: null`, `lastRequestId: null`, and `state: null`.

Reads the immutable snapshot referenced by the database record and verifies its identity, version, and request ID. It does not start a host, invoke actor code, or expose an uncommitted upload. Transient in-memory fields are unavailable. The response represents the committed record when read; a later commit can occur during inspection.

**Errors:** `400` for invalid input, `401` for a rejected admin credential, `404` for an unknown object, `500` if committed state is unavailable or inconsistent, and `503` if snapshot inspection times out.

## Storage regions

For API-key callers, new actors use `north-america-central` when configured; otherwise they use the first configured region in alphabetical order. Existing actors keep their selected region. Delegated sessions can request a different region as described below.

When a session or WebSocket callback requests a region for a new actor, the server uses an exact configured bucket-map key first. Otherwise it maps known cloud region names to these region groups:

| Group                   | Recognized aliases                                                                                                                                        |
| ----------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `north-america-east`    | `us-east-1`, `us-east-2`, `us-east1`, `us-east4`, `us-east5`, `us-ashburn-1`, `eastus`, `eastus2`                                                         |
| `north-america-central` | `us-central1`, `us-chicago-1`, `centralus`, `northcentralus`                                                                                              |
| `north-america-south`   | `us-south1`, `southcentralus`                                                                                                                             |
| `north-america-west`    | `us-west-1`, `us-west-2`, `us-west1`, `us-west2`, `us-west3`, `us-west4`, `us-phoenix-1`, `us-sanjose-1`, `westus`, `westus2`, `westus3`, `westcentralus` |
| `europe-west`           | `eu-west-1`, `eu-west-3`, `eu-central-1`, `europe-west1`, `europe-west3`, `europe-west4`, `eu-frankfurt-1`, `eu-paris-1`, `westeurope`                    |
| `asia-southeast`        | `ap-southeast-1`, `asia-southeast1`, `asia-southeast2`, `ap-singapore-1`, `southeastasia`                                                                 |

The listed GCP-style regions also accept zone suffixes `-a`, `-b`, `-c`, `-d`, and `-f`, such as `us-central1-a`.

If the requested or mapped region has no configured bucket, the server uses `north-america-central` if configured; otherwise actor startup fails. A configured fallback can also be used when initial host provisioning fails in another region. Issuing a token validates the region string's syntax; it does not prove that an actor can start there.

## Public signing keys

### GET /.well-known/jwks.json

**Response:** `200 OK` with a JSON Web Key Set containing the server's public signing key. Authentication is not required. Private signing material is never included. Consumers validating tokens must also check the expected issuer, audience, scope, and expiration.

## Direct WebSocket connections

### Backend connections

```http
GET /v1/actors/{actorType}/{actorId}/websocket
```

Upgrades to a WebSocket connection (`101 Switching Protocols`). Connect to:

```text
wss://objects.example.com/v1/actors/{actorType}/{actorId}/websocket
```

Send `Authorization: Bearer <api-key>` with the upgrade request. Use `ws://` for a local HTTP server.

Within 10 seconds of opening, send this as the first text frame:

```json
{ "type": "initialize", "metadata": { "userId": "alice" } }
```

The initialization document may be at most 64 KiB plus 128 bytes, and its metadata must fit the 64 KiB metadata limit. After initialization, send application JSON in text frames. The TypeScript runtime parses and validates each message before calling [`onMessage`](api.md#actoronmessage). It sends the automatic state message after successful acceptance. Outgoing application messages are also JSON text frames.

The SDK performs this handshake for [`reference.connect()`](api.md#referenceconnect). The browser WebSocket API cannot set the required Authorization header; use the external route below for browser connections.

### External connections

Use the [generated browser SDK and proxy helper](../../sdk/README.md#browser-clients) to manage this exchange automatically. Your proxy authenticates requests and checks actor access before asking the control plane for authorization.

```http
POST /v1/actors/{actorType}/{actorId}/socket-ticket
Authorization: Bearer <api-key>
Content-Type: application/json
```

```json
{ "metadata": { "userId": "alice" }, "authorizationLifetimeMs": 900000 }
```

Only the API key can issue tickets. Session tokens and socket tickets cannot issue them. An existing deployment is required. An optional `connectionId` requests a renewal ticket bound to that connection. Metadata is trusted backend input and limited to 64 KiB. Authorization defaults to 15 minutes, accepts 1 second through 1 day, and is capped by the issuer maximum. The response has `Cache-Control: no-store`:

```json
{ "websocketUrl": "wss://objects.example.com/v1/socket", "key": "<signed-ticket>" }
```

The URL uses the deployment's socket gateway origin when configured, otherwise the control-plane origin. Tickets authorize socket operations on exactly one actor instance; they do not authorize backend RPCs or administration. Admission expires after at most 60 seconds.

Connect with WebSocket subprotocol `little-actors.v1`. Within 10 seconds, send `{"type":"authorize","key":"<signed-ticket>"}`. Credentials are carried in the frame, not the URL. A renewal ticket cannot open a new connection.

After successful `onConnect` and persistence, the server sends `{"type":"state","state":{...},"version":1}` containing public persisted fields, then `{"type":"ready","protocol":1,"connectionId":"...","expiresInMs":900000}`. Explicit actor messages may also arrive before readiness. Application traffic uses `{"type":"message","data":...}` in both directions. Automatic changes use `{"type":"state_update","changes":{...},"removed":[],"version":2}` and contain changed `@Emittable` fields only.

Renew by obtaining a fresh ticket and sending `{"type":"renew","key":"<signed-ticket>"}` on the existing connection. The acknowledgment is `{"type":"renewed","expiresInMs":900000}`. Lifetimes are relative milliseconds. Renewal requires the same actor and, if the ticket specifies a `connectionId`, the same connection. Unchanged authorized metadata preserves actor-modified metadata and tags; changed metadata closes with `4409`, causing the SDK to reconnect and rerun `onConnect`.

Expiry is enforced while idle, receiving messages, and running handlers. Reconnect fetches a new ticket and initial snapshot. Live events have no replay, and the SDK never resends application messages.

### Message limits

Each actor supports up to 128 connections per gateway process. Application messages must be JSON text and fit 16 MiB of UTF-8, including JSON encoding overhead. The TypeScript SDK rejects binary application messages. Connection metadata is limited to 64 KiB of JSON-encoded UTF-8.

### Close behavior

| Code                | Meaning                                                                |
| ------------------- | ---------------------------------------------------------------------- |
| `1000`              | Normal closure.                                                        |
| `1002`              | Missing or invalid initialization on the backend connection route.     |
| `1006`              | An observed abnormal disconnect; not a close frame sent by the server. |
| `1011`              | Connection handling or an actor socket handler failed.                 |
| `1013`              | Actor connection limit reached.                                        |
| `4400`              | Invalid browser protocol or actor handler failure; terminal.           |
| `4401`, `4403`      | Rejected authorization or renewal target mismatch; terminal.           |
| `4408`              | Authorization expired; reconnect with fresh authorization.             |
| `4409`              | Authorized metadata changed; reconnect.                                |
| Other `3000`–`4999` | Application close or rejection; terminal.                              |

These are common runtime outcomes; WebSocket protocol and size failures may produce other standard codes. Receiving output is not an acknowledgment that a message was saved. The runtime does not replay transient broadcasts on reconnect.

## WebSocket callbacks

The optional incoming-message callback is configured on the [self-hosted server](../guides/self-hosting.md#server-configuration). The server makes JSON `POST` requests with `Authorization: Bearer <DURABLE_OBJECT_API_KEY>`. Authenticate this header at the callback endpoint. Plain local `dev` does not enable this callback.

### Incoming message events

Set `DURABLE_OBJECT_SOCKET_EVENT_URL` to receive messages after successful actor handling:

```json
{
    "eventId": "<event-id>",
    "namespaceId": "default",
    "actorType": "ChatRoom",
    "actorId": "lobby",
    "triggerId": "chat",
    "connectionId": "<connection-id>",
    "message": { "type": "text", "data": "{\"type\":\"post\",\"text\":\"Hello\"}" }
}
```

**Request fields** (all present)

- `eventId` (`string`) — Unique event ID.
- `namespaceId` (`string`), `actorType` (`string`), `actorId` (`string`) — Actor that handled the message.
- `triggerId` (`string | null`) — External route's trigger ID, or `null` for a backend connection.
- `connectionId` (`string`) — Connection that sent the message.
- `message` (`object`) — The transport envelope `{"type":"text","data":"<JSON text>"}`. Parse `message.data` to read the application value. The transport also defines a binary envelope, but the TypeScript actor runtime rejects binary application messages.

Events cover successfully handled incoming messages. Connection changes and outgoing broadcasts do not produce events.

**Response:** A successful HTTP status; no response body is required. Delivery is asynchronous and best effort, with no automatic retry or durable delivery guarantee. A callback failure is logged and does not undo the actor's completed message handling.

## Advanced scopes

The default API requires no namespace setting. For explicit scopes, deployment, target, socket-effects, WebSocket, and session-token routes also accept `/v1/namespaces/{namespaceId}` in place of `/v1`. An API key can access all namespaces on its server. Session tokens are restricted to their own namespace and cannot manage deployments or issue credentials.

Application routes also accept session bearer tokens. Without an explicit namespace in the path, they derive it from the authenticated token. Existing namespaced routes and actor identities remain supported. See [advanced access configuration](../guides/advanced-access.md).

## Session tokens

Session tokens are optional credentials for delegated workers or customer-provided code. See [advanced access configuration](../guides/advanced-access.md) for examples, including the local demo.

### POST /v1/session-scoped-token

Requires a registered deployment and the API key. This route issues a token for the default application. Use `POST /v1/namespaces/{namespaceId}/session-scoped-token` to delegate another namespace.

**JSON parameters**

- `executionId` (`string`, required) — Application execution requesting access, 1–255 bytes.
- `deadlineUnixMs` (`integer`, required) — Future Unix timestamp in milliseconds.
- `storageRegion` (`string`, required) — 1–64 lowercase ASCII letters, digits, `.`, `_`, or `-`. See [storage regions](#storage-regions) for selection behavior.

For example, from a trusted Node.js backend:

```ts
const response = await fetch(`${process.env.DURABLE_OBJECT_CONTROL_PLANE_URL}/v1/session-scoped-token`, {
    method: "POST",
    headers: {
        authorization: `Bearer ${process.env.DURABLE_OBJECT_API_KEY}`,
        "content-type": "application/json"
    },
    body: JSON.stringify({
        executionId: "chat-session-1",
        deadlineUnixMs: Date.now() + 3_600_000,
        storageRegion: "north-america-east"
    })
})
if (!response.ok) throw new Error(await response.text())
const { token, expiresAtMs } = await response.json()
```

**Response:** `200 OK` with JSON:

```json
{ "token": "<signed-session-token>", "expiresAtMs": 1800000000000 }
```

Expiration is the earliest of the requested deadline plus 30 seconds, issuance time plus the configured maximum token lifetime, and issuance time plus 24 hours. It is rounded down to whole seconds and returned as milliseconds in `expiresAtMs`. Use the returned expiration instead of calculating it yourself.

The token permits application actor operations within the requested namespace; it does not restrict access to a single class, actor, or method. It is not an admin key. There is no refresh endpoint: have your trusted backend issue a new token when needed.

**Errors:** `409` for a missing deployment, `400` for invalid fields or an expired deadline, and `401` for a rejected admin credential.

## HTTP errors

Management API handler errors have this shape:

```json
{ "error": { "code": "invalid_request", "message": "workflow deadline must be in the future" } }
```

| HTTP status | Error code        | Meaning                                           |
| ----------- | ----------------- | ------------------------------------------------- |
| `400`       | `invalid_request` | Invalid deployment or token request.              |
| `401`       | `unauthenticated` | Missing or rejected admin credential.             |
| `403`       | `forbidden`       | Credential does not permit the operation.         |
| `409`       | `conflict`        | No registered actor code for token issuance.      |
| `500`       | `internal`        | Server failure.                                   |
| `503`       | `unavailable`     | Service could not satisfy an application request. |

Malformed JSON, missing required fields, unsupported content types, oversized bodies, unknown routes, and invalid upgrade requests may be rejected before the handler. Such responses need not use the JSON error envelope; inspect the status and content type before parsing. Oversized management bodies are rejected with `413`.

WebSocket upgrade errors use a plain-text body. Common statuses are `400` for an invalid request, `401` for missing or rejected credentials, `403` for incorrect namespace access, and `503` for unavailable external authorization. After a successful upgrade, handle WebSocket events and close frames instead of HTTP errors.
