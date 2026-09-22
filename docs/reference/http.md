# HTTP and WebSocket reference

This page documents deployment management, backend access, WebSocket connections, and application callbacks. Use the [TypeScript API](api.md) for actor methods and the [self-hosting guide](../guides/self-hosting.md) to configure a server.

- [Authentication](#authentication)
- [Deployments](#deployments)
- [Object inspection](#object-inspection)
- [Storage regions](#storage-regions)
- [Public signing keys](#public-signing-keys)
- [Direct WebSocket connections](#direct-websocket-connections)
- [WebSocket callbacks](#websocket-callbacks)
- [HTTP errors](#http-errors)

## Authentication

Backend operations require:

```http
Authorization: Bearer <api-key>
```

Configure the SDK and CLI with `DURABLE_OBJECT_API_KEY` or an explicit API key. Direct HTTP callers must include the server API key in the header above; see [configuration](configuration.md) for credentials and server settings. Browser apps fetch actor-scoped connection URLs from your authenticated backend and use native WebSockets.

| Operation                      | Method and path                                      | Credential                    |
| ------------------------------ | ---------------------------------------------------- | ----------------------------- |
| Register or replace deployment | `PUT /v1/projects/{projectId}/deployment`                                 | API key                       |
| Read deployment                | `GET /v1/projects/{projectId}/deployment`                                 | API key                       |
| Read public actor contract     | `GET /v1/projects/{projectId}/deployment/contract`                        | API key                       |
| Remove deployment              | `DELETE /v1/projects/{projectId}/deployment`                              | API key                       |
| List saved actors              | `GET /v1/actors`                                     | API key                       |
| Inspect actor metadata         | `GET /v1/projects/{projectId}/actors/{actorName}/{actorId}`               | API key                       |
| Inspect committed state        | `GET /v1/projects/{projectId}/actors/{actorName}/{actorId}?include=state` | API key                       |
| Prepare a connection           | `POST /v1/projects/{projectId}/actors/{actorName}/{actorId}/connect`      | API key                       |
| Read public signing keys       | `GET /.well-known/jwks.json`                         | None                          |
| Check control-plane health     | `GET /healthz`                                       | None                          |
| Connect from an app            | `GET /v1/socket` on the actor host                   | Signed URL; WebSocket upgrade |

Call actor methods and send application broadcasts through the [TypeScript SDK](api.md). Management JSON request bodies are limited to 16 MiB; larger bodies receive `413`.

Path parameters `actorName` and `actorId` follow the [actor identity limits](api.md#identity).

## Deployments

All three operations require the API key and manage the installation's active deployment.

### PUT /v1/projects/{projectId}/deployment

Builds and registers actor code from a published customer image in one request. There is one active deployment. The JSON request replaces the complete source specification.

```json
{
    "codeRevision": "chat-v1",
    "imageRef": "im-customer-build",
    "workingDirectory": "/customer",
    "actorEntrypoint": "src/actors.ts",
    "secretRefs": []
}
```

**JSON parameters**

- `codeRevision` (`string`, required) — Revision label, 1–128 ASCII letters, digits, `.`, `_`, or `-`. Use a new label for changed code.
- `imageRef` (`string`, required) — Published Modal customer build image ID (`im-...`). The image contains the project source, installed dependencies, Bun, and the matching SDK at `/opt/durable-actors/sdk`. Base it on the matching runtime image; this endpoint does not build Docker images.
- `workingDirectory` (`string`, required) — Absolute project directory inside the build image, at most 1024 bytes.
- `actorEntrypoint` (`string | null`, default `null`) — Source entrypoint inside that image, at most 1024 bytes; defaults to `src/durable-objects.ts` relative to the project directory.
- `secretRefs` (`string[]`, default `[]`) — Up to 16 provider secret names. Each contains 1–255 ASCII letters, digits, `.`, `_`, or `-`. Secrets are attached to actor sandboxes, not the temporary compiler sandbox.
- `contract` (`object | null`, default `null`) — Optional public actor contract, up to 4 MiB. Hosted deployments generate it from the source automatically; a supplied contract must match. Different contract content for the active revision returns `409`. Local deployments can supply their compiled contract directly.

The control plane starts a temporary sandbox from the customer image, compiles the code and public contract, publishes a permanent directory snapshot, and registers the result against the configured shared runtime image. The internal bundle and snapshot are not API parameters. Build failures leave the current deployment running. The builder is terminated after success or failure.

Repeating the current image, project directory, and entrypoint reuses its compiled snapshot and contract when the shared runtime image is unchanged. Secret updates and revision-label changes do not rebuild that code.

**Response:** `200 OK` with JSON:

```json
{ "changed": true }
```

An identical deployment returns `{"changed":false}`. Changing the specification stops its previous cloud hosts before registering the replacement. Saved actor state remains, so new code must support existing state. This is not a zero-downtime rollout guarantee.

Publishing a contract for the first time also returns `{"changed":true}`. It does not restart hosts when the deployment specification is unchanged.

**Errors:** `400` for an invalid specification, contract, or failed build, `401` for a rejected admin credential, and `409` for a conflicting contract on the active revision. See [HTTP errors](#http-errors) for shared failure responses.

### GET /v1/projects/{projectId}/deployment/contract

Returns the active deployment's public actor contract. Requires the API key. No actor host needs to be running.

```text
GET /v1/projects/{projectId}/deployment/contract
GET /v1/projects/{projectId}/deployment/contract?revision=chat-v1
```

Omit `revision` to get the latest deployment's contract. The optional `revision` query checks that this revision is active and returns `404` otherwise. Only the active contract is stored; historical contracts are discarded on replacement or deletion. Unknown query parameters are rejected.

**Response:** `200 OK`, with `Cache-Control: no-store`:

```json
{
    "codeRevision": "chat-v1",
    "contractHash": "sha256:<digest>",
    "contract": { "version": 1, "actors": [] }
}
```

The example represents an empty actor API; a missing contract returns `404` with error code `not_found`. The hash identifies the contract content: SHA-256 of compact JSON with object keys sorted recursively and array order preserved. The contract contains public RPC signatures and socket schemas, without actor implementation code or credentials.

`durable-actors deploy` extracts and includes the contract automatically. Custom deployment integrations can call `ActorCompiler.compileContract()` and pass the returned object directly as `contract`. Use the same source revision as the published code snapshot. Registration validates the contract format and local type references; it does not introspect the code snapshot to verify its API.

**Errors:** `400` for an invalid revision or query; `401` for a rejected admin credential; `404` when the active deployment has no published contract or the requested revision is not active.

### GET /v1/projects/{projectId}/deployment

Reads the active deployment.

**Response:** `200 OK` with the original customer image and source paths, including its code revision. Internal runtime image and snapshot IDs are not returned, so this response can be sent back to `PUT /v1/projects/{projectId}/deployment` when changing secrets:

```json
{
    "codeRevision": "chat-v1",
    "imageRef": "im-customer-build",
    "workingDirectory": "/customer",
    "actorEntrypoint": "src/actors.ts",
    "secretRefs": []
}
```

If no deployment exists, the response is `404` with error code `not_found`.

**Errors:** `401` for a rejected admin credential; `500` if the deployment cannot be read.

### DELETE /v1/projects/{projectId}/deployment

Stops the deployment's cloud hosts and removes the active deployment registration.

**Response:** `200 OK` with `{"changed":true}`, or `{"changed":false}` if no deployment existed. It preserves saved actor state. Register actor code again before making new calls.

**Errors:** `401` for a rejected admin credential; `500` if removal fails.

There is no public actor-state deletion or individual actor reset API. Expose application-specific reset behavior as an actor method if needed.

## Object inspection

These read-only endpoints require the admin API key. Successful responses use `Cache-Control: no-store`. The [CLI](cli.md#inspect-saved-objects) uses the same endpoints for local and cloud runtimes.

### GET /v1/actors

Lists objects with committed state, including stopped objects and objects without a current deployment. Objects with no committed state are omitted.

Optional query parameters:

- `limit` — Page size from 1 to 500; defaults to 100.
- `after` — The previous response's `nextCursor`, URL-encoded.

**Response:** `200 OK` with JSON:

```json
{
    "actors": [
        {
            "actorName": "ChatRoom",
            "actorId": "lobby",
            "homeRegion": "north-america-east",
            "stateVersion": 3,
            "lastRequestId": "request-3"
        }
    ],
    "nextCursor": null
}
```

Results are ordered by storage identity. `nextCursor` is `null` on the last page; otherwise repeat the request with that cursor. Each page reads current database records, so the list is not a single snapshot across concurrent commits.

### GET /v1/projects/{projectId}/actors/{actorName}/{actorId}

**Response:** `200 OK` with the same actor metadata fields as listing. Add `?include=state` to include `state`, containing all committed persisted fields, including internal fields. The `state` field is omitted by default. An existing placement without committed state has `stateVersion: 0` and `lastRequestId: null`; requesting state returns `state: null`.

Inspection does not start a host or invoke actor code. When state is requested, it reads the immutable committed snapshot and verifies its identity, version and request ID. Transient in-memory fields are unavailable. A later commit can occur during inspection. Only `include=state` is supported; unknown query parameters are rejected.

**Errors:** `400` for invalid input, `401` for a rejected admin credential, `404` for an unknown object, `500` if committed state is unavailable or inconsistent, and `503` if snapshot inspection times out.

## Storage regions

The deployment router chooses and persists the nearest enabled region for a new actor. It sends `homeRegion` to the selected regional control plane in `/connect` requests. The runtime does not select a region from caller location or query a routing database.

When `homeRegion` is omitted, new actors use `DURABLE_OBJECT_REGION`, or `north-america-central` if the control plane is unpinned. Existing actors keep their persisted home. An explicit assignment must match the control plane's configured region and any existing actor placement; a conflict returns `409`. A failed provisioning attempt does not move an actor elsewhere.

### POST /v1/projects/{projectId}/actors/{actorName}/{actorId}/connect

Requires the API key. Select `transport: "grpc"` or `transport: "websocket"`. Unknown fields and fields belonging to the other transport are rejected. Setup can provision and activate an actor host.

For backend gRPC calls:

```json
{ "transport": "grpc", "homeRegion": "north-america-west" }
```

**Response:** `200 OK`, with `Cache-Control: no-store`:

```json
{
    "transport": "grpc",
    "homeRegion": "north-america-west",
    "route": "https://actor-host.example.com",
    "token": "<actor-invocation-capability>",
    "ownerEpoch": 1,
    "expiresAtMs": 1900000000000
}
```

The SDK uses this capability for direct gRPC calls to the host. It authorizes the named actor and ownership epoch. `expiresAtMs` is a Unix timestamp in milliseconds. The WebSocket request and response are described below.

## Internal gRPC services

Internal operations use authenticated gRPC over HTTP/2. They have no public JSON endpoints. The [protobuf contract](../../proto/durable_object.proto) defines:

| Service                    | Operations                                                         | Caller                               |
| -------------------------- | ------------------------------------------------------------------ | ------------------------------------ |
| `ActorHostService`         | `Activate`, `Invoke`, `HandleSocket`, `PublishSocketEffects`       | Runtime and backend SDK              |
| `ActorControlPlaneService` | `Execute`: host registration, storage access and write preparation | Actor hosts                          |
| `SnapshotService`          | `Read`, `Write`                                                    | Actor hosts and recovery runtime     |
| `ReplicaService`           | `Initialize`, `Head`, `Seal`                                       | Ownership and recovery runtime       |

Socket effects go directly to the owning host and are checked against its actor, host session and ownership epoch. Snapshot and replica calls carry signed, operation-scoped capabilities in gRPC metadata. Storage capability addresses use `grpc://` or `grpcs://`; the runtime resolves them to HTTP/2 connections. Public health checks, JWKS and application callbacks remain HTTP.

## Public signing keys

### GET /.well-known/jwks.json

**Response:** `200 OK` with a JSON Web Key Set containing the server's public signing key. Authentication is not required. Private signing material is never included. Consumers validating tokens must also check the expected issuer, audience, scope, and expiration.

## Direct WebSocket connections

### Browser connections

Your backend checks user access, then calls the generated `actors.ChatRoom.prepareWebsocket({ actorId, metadata })` helper. It issues a signed grant through this API:

```http
POST /v1/projects/{projectId}/actors/{actorName}/{actorId}/connect
Authorization: Bearer <api-key>
Content-Type: application/json
```

```json
{ "transport": "websocket", "metadata": { "userId": "alice" }, "authorizationLifetimeMs": 900000 }
```

Grants require the backend API key. The helper resolves `projectId` from its options or `DURABLE_OBJECT_PROJECT_ID`. The current admin key has installation-wide authority; project routing and actor-bound tickets do not replace tenant-scoped issuance authorization. A hosted service must restrict which projects each issuing credential can access. Your application proxy authenticates the customer and decides which actor they may access. An existing deployment is required; issuing a grant can provision and activate its actor host. Metadata is trusted backend input and limited to 64 KiB. Authorization defaults to 15 minutes, accepts 1 second through 1 day, and is capped by the issuer maximum. Setup accepts the optional `homeRegion` assignment described above. The response has `Cache-Control: no-store`:

```json
{
    "transport": "websocket",
    "homeRegion": "north-america-west",
    "websocketUrl": "wss://<modal-host>/v1/socket?_modal_connect_token=<modal-token>&key=<signed-ticket>",
    "connectByMs": 1900000060000,
    "authorizedUntilMs": 1900000900000
}
```

The URL comes from the owning actor host's provider; local development returns a local host URL without a Modal token. Preserve the whole URL. Its signed key binds the project ID, actor name, and actor ID to the owning host, session, and ownership epoch; it does not authorize backend RPCs or administration. Open the URL before `connectByMs` (normally within 60 seconds). `authorizedUntilMs` is the connection authorization deadline. Both are Unix timestamps in milliseconds. The URL and both tokens are credentials; omit them from access logs.

Pass the URL directly to a native WebSocket:

```js
const socket = new WebSocket(grant.websocketUrl)
socket.onopen = () => socket.send(JSON.stringify({ type: "post", text: "Hello" }))
socket.onmessage = event => console.log(JSON.parse(event.data))
```

The actor host verifies the key and host binding before upgrading. Missing, invalid, expired, or stale host-bound keys receive HTTP `401`. No subprotocol, authorization frame, or readiness frame is required. Messages sent immediately after the browser's `open` event wait for the actor's `onConnect` handler to finish.

Application JSON travels directly in text frames in both directions. The actor host validates incoming messages. Public `@Persisted @Emittable` fields synchronize automatically: a connection receives `{"type":"state","state":{"count":0},"version":1}`, then committed changes such as `{"type":"state_update","changes":{"count":1},"removed":[],"version":2}`. Apply `changes` and delete fields listed in `removed`. Private, protected, and non-emittable fields never enter these messages. Actors without emittable fields send no automatic state frames. Explicit messages from `socket.send()` and `this.broadcast()` share the connection; `state` and `state_update` are reserved message types.

Authorization expires even while idle or running a handler; the host closes the connection with `4408`. There is no renewal protocol or automatic reconnect. To reconnect, your application obtains another grant and creates another WebSocket. Transient messages are not replayed.

### Message limits

Each actor supports up to 128 connections on its host. Application messages must be JSON text and fit 16 MiB of UTF-8, including JSON encoding overhead. The TypeScript SDK rejects binary application messages. Connection metadata is limited to 64 KiB of JSON-encoded UTF-8. A slow consumer whose 32-message output queue fills is disconnected.

### Close behavior

| Code                | Meaning                                                                |
| ------------------- | ---------------------------------------------------------------------- |
| `1000`              | Normal closure.                                                        |
| `1002`              | Missing or invalid initialization on the backend connection route.     |
| `1006`              | An observed abnormal disconnect; not a close frame sent by the server. |
| `1011`              | Connection handling or an actor socket handler failed.                 |
| `1012`              | Host stopping or ownership lease lost; obtain a fresh grant.           |
| `1013`              | Actor connection limit or output queue limit reached.                  |
| `4400`              | Invalid application message or actor handler failure.                  |
| `4408`              | Authorization expired; reconnect with fresh authorization.             |
| Other `3000`–`4999` | Application close or rejection; terminal.                              |

These are common runtime outcomes; WebSocket protocol and size failures may produce other standard codes. Receiving output is not an acknowledgment that a message was saved. The runtime does not replay transient broadcasts on reconnect.

## WebSocket callbacks

Enable incoming-message callbacks in the [server configuration](configuration.md). The server makes JSON `POST` requests with `Authorization: Bearer <api-key>`. Authenticate this header at the callback endpoint. Plain local `dev` does not enable this callback.

### Incoming message events

After successful actor handling, the callback receives:

```json
{
    "eventId": "<event-id>",
    "actorName": "ChatRoom",
    "actorId": "lobby",
    "triggerId": "chat",
    "connectionId": "<connection-id>",
    "message": { "type": "text", "data": "{\"type\":\"post\",\"text\":\"Hello\"}" }
}
```

**Request fields** (all present)

- `eventId` (`string`) — Unique event ID.
- `actorName` (`string`), `actorId` (`string`) — Actor that handled the message.
- `triggerId` (`string | null`) — External route's trigger ID, or `null` for a backend connection.
- `connectionId` (`string`) — Connection that sent the message.
- `message` (`object`) — The transport envelope `{"type":"text","data":"<JSON text>"}`. Parse `message.data` to read the application value. The transport also defines a binary envelope, but the TypeScript actor runtime rejects binary application messages.

Events cover successfully handled incoming messages. Connection changes and outgoing broadcasts do not produce events.

**Response:** A successful HTTP status; no response body is required. Delivery is asynchronous and best effort, with no automatic retry or durable delivery guarantee. A callback failure is logged and does not undo the actor's completed message handling.

## HTTP errors

Management API validation and handler errors have this shape:

```json
{ "error": { "code": "invalid_request", "message": "socket authorization lifetime must be between one second and one day" } }
```

| HTTP status | Error code          | Meaning                                           |
| ----------- | ------------------- | ------------------------------------------------- |
| `400`       | `invalid_request`   | Invalid deployment or setup request.              |
| `401`       | `unauthenticated`   | Missing or rejected admin credential.             |
| `404`       | `not_found`         | Deployment, contract or actor not found.          |
| `413`       | `payload_too_large` | Management body exceeds 16 MiB.                   |
| `409`       | `conflict`          | Conflicting contract or home-region assignment.   |
| `500`       | `internal`          | Server failure.                                   |
| `503`       | `unavailable`       | Service could not satisfy an application request. |

Malformed JSON, missing required fields and invalid queries return `400`; oversized management bodies return `413`. Routing, path decoding and WebSocket upgrade errors may be rejected before the management handler and need not use the JSON envelope. Inspect the status and content type before parsing.

WebSocket upgrade errors use a plain-text body. Common statuses are `400` for an invalid request, `401` for missing or rejected credentials, `503` for an unavailable actor host. After a successful upgrade, handle WebSocket events and close frames instead of HTTP errors.
