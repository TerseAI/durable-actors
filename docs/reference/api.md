# TypeScript API reference

- [Actor](#actor)
- [Actor references](#actor-references)
- [ActorSocket](#actorsocket)
- [ActorConnection](#actorconnection)
- [ActorInvocationError](#actorinvocationerror)
- [Types](#types)

## Actor

```ts
import { Actor, Persisted } from "little-actors"

export class ChatRoom extends Actor {
    @Persisted history: string[] = []

    async post(message: string): Promise<number> {
        this.history.push(message)
        this.broadcast(message)
        return this.history.length
    }
}
```

Classes must extend `Actor` directly and have no required constructor arguments. The base constructor and `get()` method is protected.

Remote methods must be `async` prototype methods.

### Actor.get

```text
static get(actorId: string)
```

Creates a typed reference to an actor of the subclass. Creating a reference is synchronous and makes no remote call. The first operation starts the actor if needed.

**Parameters**

- `actorId` (`string`, required) — Actor ID within the actor class. See [identity](#identity) for validation.

**Returns:** An inferred [actor reference](#actor-references), exposing the subclass's asynchronous application methods, `connect()`, and `broadcast()`.

**Raises:** An `Error` if the actor definition or ID is invalid.

```ts
import { ChatRoom } from "./durable-objects.js"

const room = ChatRoom.get("lobby")
await room.post("Hello")
```

### [Actor.id](http://Actor.id)

```text
protected readonly id: string
```

The current actor ID, available inside an actor method or lifecycle hook. Reading it before the runtime binds the actor, including in its constructor, raises an `Error`. It is not a saved state field.

### Actor.getConnections

```text
protected getConnections(): Promise<readonly ActorSocket<Metadata, Outgoing, Tag>[]>
```

Returns connections inside an actor method or lifecycle hook. Includes the connecting socket during `onConnect` and excludes the disconnected socket during `onDisconnect`. Calling it outside an invocation rejects with an `Error`. Each actor supports up to 128 connections per gateway process.

Ordinary methods fetch connections from the gateway only when `getConnections()` is called. Repeated or concurrent calls share one lookup and the same socket objects within that invocation. Lifecycle hooks use the snapshot supplied with their event. A failed lookup rejects the promise; uncaught errors prevent the invocation from committing.

### Actor.broadcast

```text
protected broadcast(message: Outgoing, options?: ActorBroadcastOptions<Tag>): void
```

Sends to currently open connections. By default, this includes the sender when called from `onMessage`.

**Parameters**

- `message` ([ActorSocketMessage](#actorsocketmessage), required) — JSON value matching the actor's outgoing message type.
- `options` ([ActorBroadcastOptions](#actorbroadcastoptions), optional) — Recipient exclusions and tag filters. Omitted options send to all open connections.

**Returns:** `void`. Recipients do not acknowledge delivery, and broadcasting does not add the message to saved state.

**Raises:** An `Error` outside an invocation, for invalid messages or tags, or if socket output exceeds its [limits](#socket-output-limits).

Inside an actor method or hook:

```ts
this.broadcast({ type: "chat", text: "Hello" }, { except: socket })
this.broadcast({ type: "updated" }, { tags: ["editors", "document-1"] })
this.broadcast({ type: "files-ready" }, { tags: ["file:a", "file:b"], tagMatch: "any" })
```

### Actor.onConnect

```text
async onConnect(socket: ActorSocket<Metadata, Outgoing, Tag>): Promise<void>
```

Optional lifecycle hook called when a connection is initialized. The socket is `"connecting"`; acceptance is automatic when the hook succeeds without rejecting it. Calling `socket.reject(4003, "Access denied")` suppresses acceptance and initial state delivery.

**Parameters**

- `socket` ([ActorSocket](#actorsocket)) — Joining connection. Its metadata, outgoing messages, and tags follow the actor generics.

**Returns:** `Promise<void>`. Successful state changes are saved.

### Actor.onMessage

```text
async onMessage(socket: ActorSocket<Metadata, Outgoing, Tag>, message: Incoming): Promise<void>
```

Optional lifecycle hook called for incoming application messages. The runtime parses JSON and validates it against the optional incoming schema before calling the hook. Incoming activity wakes a hibernating actor as needed.

**Parameters**

- `socket` ([ActorSocket](#actorsocket)) — Sending connection, in state `"open"`.
- `message` (`Incoming`) — Parsed JSON matching the actor's incoming message type.

**Returns:** `Promise<void>`. Successful state changes are saved.

### Actor.onDisconnect

```text
async onDisconnect(socket: ActorSocket<Metadata, Outgoing, Tag>, code: number, reason: string, wasClean: boolean): Promise<void>
```

Optional lifecycle hook called when the server observes a connection closing. The socket is absent from `await this.getConnections()` and cannot send messages.

**Parameters**

- `socket` ([ActorSocket](#actorsocket)) — Closed connection, in state `"closed"`.
- `code` (`number`) — Observed WebSocket close code.
- `reason` (`string`) — Close text.
- `wasClean` (`boolean`) — Whether a clean close was observed.

**Returns:** `Promise<void>`. Successful state changes are saved.

An abrupt server failure can prevent the hook from running. Connections can remain open while an actor hibernates, but clients must reconnect after a gateway restart. Connection metadata and tags are not restored as a new connection after a restart.

### Identity

An actor's class name and actor ID identify its saved state. Reusing that identity addresses the same actor. Changing the class name or actor ID addresses a different actor.

| Component        | Maximum length |
| ---------------- | -------------- |
| Actor class name | 48 bytes       |
| Actor ID         | 128 bytes      |
| Method name      | 128 bytes      |

Each component must be nonempty and contain only ASCII letters, digits, `.`, `_`, and `-`. Validation can occur when creating a reference or when a request reaches the server.

### Saved state and serialization

Stack `@Emittable` with `@Persisted` on a public field to synchronize it with connected browsers. Connections receive an initial `state` snapshot of those fields, then `state_update` messages after successful commits. Nested mutations are detected and coalesced once per operation. Private, protected, and non-emittable fields are excluded. Actors without emittable fields send no automatic state messages. Use `socket.send()` and `this.broadcast()` for application messages.

Every instance field must declare exactly one of `@Persisted` or `@Ephemeral`, imported from `little-actors`. Actor startup checks the TypeScript declarations, including aliased imports and re-exports. Missing, duplicate, or conflicting annotations fail before the actor module executes.

```ts
import { Actor, Ephemeral, Persisted } from "little-actors"

export class Counter extends Actor {
    @Persisted count = 0
    @Ephemeral private cache = new Map<string, number>()

    async increment(): Promise<number> {
        return ++this.count
    }
}
```

Only `@Persisted` fields are saved after successful invocations and lifecycle events. Restoration constructs a fresh instance and overlays saved values for its declared persisted fields. Missing persisted fields keep their initializer defaults. Unknown saved keys are ignored. Ephemeral fields retain their fresh initial values even if a snapshot contains a key with the same name.

Ephemeral values survive calls while the instance remains resident. Eviction, restart, or reconstruction after a failed call resets them. They can hold caches, connections, and values that cannot be serialized. Static fields are outside actor state. TypeScript `private` and `protected` fields follow their annotation; JavaScript `#private` fields require `@Ephemeral`.

Use standard TypeScript decorators without parentheses. Persistence decorators cannot target methods, accessors, static fields, symbol keys, or `declare` fields. Constructor parameter properties must become decorated class fields. Adding undeclared own properties dynamically fails at construction or when saving state.

Remote method arguments, results, and saved state use JSON serialization:

| Value                                                            | Serialization behavior                                                                               |
| ---------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| Strings, booleans, finite numbers, `null`, arrays, plain objects | Preserved as JSON values.                                                                            |
| `Date`                                                           | Becomes its JSON string representation.                                                              |
| `undefined`, functions, symbols in object properties             | Omitted.                                                                                             |
| `undefined`, functions, symbols in arrays                        | Become `null`.                                                                                       |
| `NaN`, positive or negative infinity                             | Become `null`.                                                                                       |
| `BigInt` or circular references                                  | Fail serialization.                                                                                  |
| `Map`, `Set`, custom classes                                     | Serialize their enumerable properties or `toJSON()` result; prototypes and methods are not retained. |

A method returning `undefined` produces `null` at runtime. TypeScript return annotations do not change JSON behavior. Socket messages and metadata use stricter JSON validation: unsupported values such as `undefined`, `Date`, `BigInt`, non-finite numbers, bytes, and circular references are rejected before sending.

When an actor method or socket lifecycle hook throws, its state changes are not saved. The worker stays alive and reconstructs the actor from its pre-request persisted state before returning the error; ephemeral fields reset. A worker crash or failed reconstruction still requires eviction. External effects, including HTTP requests and already sent WebSocket messages, cannot be rolled back. Socket output can arrive before state is committed; receiving a broadcast does not confirm persistence.

Signed browser connections receive automatic snapshots and updates of public `@Emittable` fields, alongside explicit application messages. Private, protected, and non-emittable fields are excluded.

Saved state is limited to 16 MiB of JSON. Method requests and responses must fit 32 MiB, including encoded state, arguments or results, and message overhead. Individual limits do not guarantee that a near-limit combination fits in one request.

### Actor validation

The host validates actor definitions at startup. To validate actors without starting the host:

```ts
import { ActorCompiler } from "little-actors/compiler"

const compiler = new ActorCompiler()
compiler.check("src/durable-objects.ts")
```

Actor entrypoints must be TypeScript source files. `compiler.check(entrypoint)` reads `tsconfig.json`, checks TypeScript and actor definitions, and returns persistence schemas. Pass `{ configFile: "path/to/tsconfig.json" }` as the second argument to select another configuration. Compiler tooling is loaded separately from the normal actor API.

Returned field schemas use `Persistence.Persisted` or `Persistence.Ephemeral`. Import the `Persistence` enum from `little-actors/compiler` when inspecting those schemas.

## Actor references

References are inferred from the subclass passed to `[Actor.get()](#actorget)`; there is no separately exported `ActorReference` type. They expose asynchronous application methods, `connect()`, and `broadcast()`. Saved fields and lifecycle hooks are not exposed.

### Remote methods

Application methods retain their declared parameter and return types on the reference. Arguments and results follow the [JSON serialization rules](#saved-state-and-serialization). To read a field remotely, expose a method that returns it.

Calls to the same actor execute one at a time, including across `await` within a method. Separate actors have independent state and can execute concurrently. Concurrent requests are serialized, but their launch order does not guarantee their execution order.

**Returns:** A promise for the method's result. Successful calls save state.

**Raises:** `[ActorInvocationError](#actorinvocationerror)` for failed remote operations, or an ordinary `Error` for definition, configuration, validation, or serialization failures.

Actor-to-actor remote calls, connections, and broadcasts are not supported inside actor invocations. Multiple actors can be coordinated from application code. Work that affects an invocation must be awaited; background work is not a durable job mechanism.

### reference.connect

```text
connect(metadata: Metadata): Promise<ActorConnection<Incoming, Outgoing, JsonObject>>
```

Opens a WebSocket connection to the actor. The SDK handles bearer authentication and initialization.

**Parameters**

- `metadata` (required) — JSON connection metadata matching the actor's first generic parameter, even when no lifecycle hooks are defined. Use `{}` for no metadata with an untyped actor.

**Returns:** `Promise<ActorConnection>`, resolving when the WebSocket opens and initialization is sent. The actor's `onConnect` hook and initial state delivery can still be pending.

**Raises:** An `Error` if setup or opening fails. After opening, failures arrive through [connection events](#connection-events); a hook can still reject the connection.

```ts
import { ChatRoom } from "./durable-objects.js"

const socket = await ChatRoom.get("lobby").connect({})
socket.addEventListener("message", ({ data }) => console.log(data))
```

Install message and close listeners immediately after awaiting `connect()`. A reconnect requires a new call with fresh metadata; neither automatic reconnect nor transient-message replay is provided.

### reference.broadcast

```text
broadcast(message: Outgoing): Promise<void>
```

Sends transient output to the actor's currently connected clients. It does not execute an actor method, update saved state, or retain the message for future connections. There are no recipient-filter options.

**Parameters**

- `message` ([ActorSocketMessage](#actorsocketmessage), required) — JSON value matching the actor's outgoing message type.

**Returns:** `Promise<void>`. With no connected clients, there is nothing to deliver.

**Raises:** An `Error` for invalid input or configuration, or an `[ActorInvocationError](#actorinvocationerror)` for a server-reported failure.

```ts
import { ChatRoom } from "./durable-objects.js"

await ChatRoom.get("lobby").broadcast({ type: "notice", text: "Deployment completed" })
```

To save and broadcast together, invoke an actor method that updates a field and calls `[this.broadcast()](#actorbroadcast)`.

## ActorSocket

```ts
import type { ActorSocket } from "little-actors"
```

Actor-side connection passed to lifecycle hooks and listed in `await this.getConnections()`. `ActorSocket<Metadata, Outgoing, Tag>` describes metadata, sent messages, and tags. Defaults are JSON values for metadata and messages, and `string` for tags. Prefer `ActorSocketOf<YourActor>` to reuse the actor declaration. Import it as a type; it is not a constructor.

### [ActorSocket.id](http://ActorSocket.id)

```text
readonly id: string
```

Unique connection ID.

### ActorSocket.metadata

```text
metadata: Metadata
```

JSON metadata supplied at connection time. Assign the whole value to retain a change for later events; mutating a nested property alone does not publish an update. Assignment raises an `Error` if the value is not JSON or fails the metadata schema.

Inside a hook using `ActorSocket<{ userId: string }>`:

```ts
socket.metadata = { ...socket.metadata, userId: "bob" }
```

Metadata is limited to 64 KiB of JSON-encoded UTF-8. SDK clients supply their own metadata; it is not proof of identity or permission. It lasts for the connection unless copied into saved actor fields.

### ActorSocket.tags

```text
readonly tags: readonly Tag[]
```

Current connection tags. Replace them with `[setTags()](#actorsocketsettags)`. Tags last for the connection and are not saved actor fields.

### ActorSocket.state

```text
readonly state: ActorSocketState
```

Connection state: `"connecting"` during `onConnect`, `"open"` during message handling, or `"closed"` after closing. See [ActorSocketState](#actorsocketstate-type).

### ActorSocket.send

```text
send(message: Outgoing): void
```

Sends a JSON value to this connection, including during `onConnect`. The SDK validates and encodes it automatically.

**Parameters**

- `message` (`Outgoing`, required) — JSON value matching the actor's outgoing message type.

**Returns:** `void`.

**Raises:** An `Error` if the socket is closed, the message type is invalid, or socket output exceeds its [limits](#socket-output-limits).

Inside an actor method or hook:

```ts
socket.send({ type: "chat", text: "Hello" })
```

### ActorSocket.close

```text
close(code?: number, reason?: string): void
```

Closes the connection. Repeating a valid close on an already closed socket does nothing.

**Parameters**

- `code` (`number`, default `1000`) — Integer `1000` or an application code from `3000` through `4999`.
- `reason` (`string`, default `""`) — Close text, at most 123 UTF-8 bytes.

**Returns:** `void`.

**Raises:** An `Error` for an invalid code or an oversized reason, including when the socket is already closed.

### ActorSocket.reject

```text
reject(code?: number, reason?: string): void
```

Rejects a joining connection during `onConnect` and suppresses the automatic state message.

**Parameters**

- `code` (`number`, default `1008`) — Subject to the same validation as `[close()](#actorsocketclose)`.
- `reason` (`string`, default `"connection rejected"`) — Close text, at most 123 UTF-8 bytes.

**Returns:** `void`.

**Raises:** An `Error` unless the socket is `"connecting"`, or if the code or reason is invalid.

The current default code `1008` fails close-code validation. Pass an accepted application code explicitly:

```ts
socket.reject(4003, "Access denied")
```

### ActorSocket.setTags

```text
setTags(...tags: Tag[]): void
```

Replaces connection tags, removing duplicates. Calling with no arguments clears all tags.

**Parameters**

- `tags` (`Tag[]`) — Up to 128 unique tags, each 1–256 JavaScript string code units. Total tag text must fit 8 KiB of UTF-8. Values must pass the optional `schemas.tag` validator.

**Returns:** `void`.

**Raises:** An `Error` for invalid tags or exceeded limits.

```ts
socket.setTags("editors", "document-1")
```

### Socket output limits

JSON text messages are limited to 16 MiB of UTF-8, including JSON encoding overhead. Binary application messages are unsupported. Pending actor socket output is limited to 512 queued operations or 24 MiB of serialized queued output. Exceeding the queue limit fails the operation.

Sending and broadcasting do not acknowledge persistence or recipient delivery. Already sent output cannot be rolled back if the actor later fails.

## ActorConnection

```ts
import type { ActorConnection } from "little-actors"
```

Connection returned by the server SDK's `[reference.connect()](#referenceconnect)`. The SDK handles authentication, initialization, JSON encoding and decoding, and declared schema validation. `ActorConnection<Send, Receive, State>` types sent messages, received application messages, and initial state. `connect()` infers send and receive types from the actor and uses `JsonObject` for initial state. Application code handles display and replay beyond the initial saved state. Import it as a type; it is not a constructor.

### ActorConnection.readyState

```text
readonly readyState: number
```

WebSocket state: `0` connecting, `1` open, `2` closing, or `3` closed. The connection is already open when `connect()` resolves.

### ActorConnection.send

```text
send(data: Send): void
```

Sends application data to the actor's `onMessage` hook. Send only while the connection is open.

**Parameters**

- `data` (`Send`, required) — JSON value matching the actor's incoming message type. Pass objects directly; do not call `JSON.stringify()` first.

**Returns:** `void`. This is not an acknowledgment of actor handling or persistence.

**Raises:** An `Error` if data is not JSON, fails the incoming schema, exceeds the message limit, or the WebSocket is still connecting. Later transport failures arrive through connection events.

### ActorConnection.close

```text
close(code?: number, reason?: string): void
```

Starts the WebSocket closing handshake. Repeating a close after the connection has closed does nothing.

**Parameters**

- `code` (`number`, optional) — WebSocket close code. Omitted sends no explicit status code; `1000` requests normal closure. Standard sendable WebSocket codes and application codes `3000`–`4999` are accepted.
- `reason` (`string`, default `""`) — Close text, at most 123 UTF-8 bytes.

**Returns:** `void`.

**Raises:** An `Error` for an invalid close code or oversized reason when starting the handshake.

Client connections do not have the actor-side `reject()` method.

### ActorConnection.addEventListener

```text
addEventListener(type, listener): void
```

Registers an event listener. Event fields are inferred from the event name.

**Parameters**

- `type` (`"open" | "message" | "close" | "error"`, required) — [Connection event](#connection-events) to listen for.
- `listener` (function, required) — Receives the corresponding event object and returns `void`.

**Returns:** `void`. The public interface does not accept listener options.

### ActorConnection.removeEventListener

```text
removeEventListener(type, listener): void
```

Removes an event listener.

**Parameters**

- `type` (`"open" | "message" | "close" | "error"`, required) — Event name used when registering.
- `listener` (function, required) — The same function passed to `addEventListener()`.

**Returns:** `void`.

### Connection events

| Event     | Fields                                                                 | Meaning                                                                       |
| --------- | ---------------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| `open`    | `type: "open"`                                                         | Connection opened. Normally occurs before `connect()` returns the connection. |
| `message` | `type: "message"`, `data: Receive                                      | ActorStateMessage`                                                            |
| `close`   | `type: "close"`, `code: number`, `reason: string`, `wasClean: boolean` | Connection closed.                                                            |
| `error`   | `type: "error"`                                                        | Connection error. The public event type has no message field.                 |

The initial state arrives as a parsed object with shape `{"type":"state","state":{...}}`. Its state contains the actor's persisted fields and has the `JsonObject` type; narrow field values before using them. Application messages are parsed and checked against the outgoing schema. Malformed JSON or invalid messages cause an `error` event and closure with code `1007`; binary frames close with `1003`. See [close behavior](http.md#close-behavior) for common server codes.

## ActorInvocationError

```ts
import { ActorInvocationError } from "little-actors"
```

An `Error` subclass for failed remote operations. Its `name` is `"ActorInvocationError"`.

```text
new ActorInvocationError(code: string, requestId: string, message: string)
```

**Parameters**

- `code` (`string`, required) — Error category.
- `requestId` (`string`, required) — ID of the caller attempt.
- `message` (`string`, required) — Failure description, available through the inherited `message` property.

```ts
import { ActorInvocationError } from "little-actors"

import { ChatRoom } from "./durable-objects.js"

try {
    await ChatRoom.get("lobby").post("Hello")
} catch (error) {
    if (error instanceof ActorInvocationError) {
        console.error(error.code, error.requestId, error.message)
    }
    throw error
}
```

### ActorInvocationError.code

```text
readonly code: string
```

Server-reported error category. This is an open string, not a closed enum; additional codes can occur.

| Code                 | Meaning                                                                                          |
| -------------------- | ------------------------------------------------------------------------------------------------ |
| `unauthenticated`    | API key or actor capability rejected. HTTP `401` and `403` during method calls map to this code. |
| `actor_error`        | Actor execution failed, including user exceptions or invalid output.                             |
| `resource_exhausted` | Execution resource limit reached.                                                                |
| `unavailable`        | Actor could not be reached or made available.                                                    |
| `outcome_unknown`    | Caller could not confirm the result; the operation may have run and saved state.                 |
| `invalid_request`    | Invalid request reported by the server.                                                          |
| `conflict`           | Deployment conflict reported by the server.                                                      |
| `internal`           | Server failure.                                                                                  |

### ActorInvocationError.requestId

```text
readonly requestId: string
```

Identifies the caller attempt. It is not an application idempotency key.

### Errors and retries

The SDK can recover from a changed actor location, but application operations are not automatically idempotent. Retrying an uncertain outcome can run an operation twice. An application can store a caller-supplied operation ID and its result in actor state to recognize duplicates.

Validation, actor definition, configuration, serialization, and socket failures can be ordinary `Error` instances. `ActorInvocationError` is the only application error class exported from the package root. Other errors need not have `code` or `requestId`.

## Types

These types are exported from `little-actors` alongside `ActorSocket` and `ActorConnection`.

### ActorSocketOf and ActorMessageOf

`ActorSocketOf<Instance>` derives an actor-side socket's metadata, outgoing message, and tag types. `ActorMessageOf<Instance>` derives the incoming message type. Pass the actor instance type, such as `ChatRoom`, rather than `typeof ChatRoom`.

### ActorSchemas

`ActorSchemas<Metadata, Incoming, Outgoing = Incoming, Tag extends string = string>` describes optional Zod validators named `metadata`, `incoming`, `outgoing`, and `tag`. See [generics and wire validation](#generics-and-wire-validation).

### ActorStateMessage

```text
interface ActorStateMessage<State> {
    readonly type: "state"
    readonly state: State
}
```

The automatic initial state message, included in the client message event union. Narrow on `data.type === "state"` when using an application message union with distinct `type` values.

### ActorClass

```ts
import type { ActorClass } from "little-actors"
```

```text
type ActorClass<Instance extends Actor<unknown, unknown, unknown> = Actor<unknown, unknown, unknown>> = Function & {
    readonly prototype: Instance
    readonly schemas?: ActorSchemas
}
```

An actor class whose prototype has type `Instance`. The type describes the class object; runtime discovery still enforces the [actor definition rules](#actor).

### ActorBroadcastOptions

```ts
import type { ActorBroadcastOptions } from "little-actors"
```

`ActorBroadcastOptions<Tag = string>` supplies recipient filters for `[Actor.broadcast()](#actorbroadcast)`. Both properties are readonly and optional; filters and exclusions can be combined.

#### ActorBroadcastOptions.except

```text
readonly except?: Pick<ActorSocket, "id"> | readonly Pick<ActorSocket, "id">[]
```

Connections to exclude. Defaults to none; at most 128 exclusions are allowed.

#### ActorBroadcastOptions.tags

```text
readonly tags?: readonly Tag[]
```

Deliver only to connections matching the listed tags according to `tagMatch` (default: `"all"`). Omitted or empty means no tag filter in either mode. Each tag must contain 1–256 JavaScript string code units and pass the actor's tag schema. The same count and byte limits as `setTags()` apply.

#### ActorBroadcastOptions.tagMatch

```ts
readonly tagMatch?: "all" | "any"
```

Controls how `tags` selects recipients. `"all"` requires every listed tag and is the default. `"any"` requires at least one listed tag. Each matching connection receives the message once, even if several tags match. `except` excludes connections in either mode.

### ActorSocketMessage

```text
type ActorSocketMessage = string | number | boolean | null
    | readonly ActorSocketMessage[]
    | { readonly [key: string]: ActorSocketMessage }
```

The default JSON message type. Strings are encoded as JSON strings; raw text and binary frames are unsupported by the SDK. Import with `import type { ActorSocketMessage } from "little-actors"`.

### ActorSocketState (type)

```text
type ActorSocketState = "connecting" | "open" | "closed"
```

Actor-side connection state. Import with `import type { ActorSocketState } from "little-actors"`. Client connections instead expose numeric `[readyState](#actorconnectionreadystate)`.
