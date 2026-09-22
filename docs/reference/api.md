# TypeScript API

The reference is generated from TSDoc comments with TypeDoc. Run `pnpm docs:build` from the repository root and open `.artifacts/api/index.html`. The same comments appear in editor hover help.

## Define and call an actor

Export actors from `src/durable-objects.ts`:

```ts
import { Actor, Persisted } from "little-actors"

export class Counter extends Actor {
    @Persisted count = 0

    async increment(): Promise<number> {
        return ++this.count
    }
}
```

Call it from your backend using the [connection settings](configuration.md):

```ts
const count = await Counter.get("visits").increment()
```

Extend `Actor` directly, without required constructor arguments. Methods must be async. Calls to one actor run sequentially by default. Await all work before returning; call other actors from your backend.

The project, class name, and actor ID identify saved state. Reuse them to access the same actor. IDs accept 1–128 ASCII letters, digits, `.`, `_`, or `-`.

## Saved state

Every instance field needs one persistence decorator. Use decorators without parentheses.

| Decorator    | Use                                                               |
| ------------ | ----------------------------------------------------------------- |
| `@Persisted` | Save the field after successful calls.                            |
| `@Ephemeral` | Keep a temporary value that may reset between calls.              |
| `@Emittable` | Add to a public `@Persisted` field to send browser state updates. |

Use JSON values for saved state, arguments, and results. Failed calls roll back saved-state changes, except in actors with reentrant methods. External effects and sent messages cannot be undone; receiving a message does not confirm persistence.

## Connections and socket output

Use `onConnect`, `onMessage`, and `onDisconnect` to handle connections. Send JSON values with `socket.send(message)` or `this.broadcast(message)`. Reject a joining connection with `socket.reject(4003, "Access denied")`.

`Actor<Metadata, Incoming, Outgoing = Incoming, Tag extends string = string>` types connections. Use `ActorSocketOf<YourActor>` and `ActorMessageOf<YourActor>` for hook parameters. Optional static `schemas` of type `ActorSchemas` provide Zod validation.

Browsers connect through your backend; see [WebSockets](../guides/websockets.md). Backend code can use `YourActor.get(id).connect(metadata)` and attach listeners immediately. Handle reconnection in your application.

## Reentrant methods (experimental)

`@Reentrant` lets other calls run while a method awaits. State may change across an `await`. Using it disables error rollback for the **whole actor class**: failed-call changes may be saved by another successful call. State is saved when calls complete successfully, not at each `await`.

## Errors and retries

Remote failures throw `ActorInvocationError`; inspect `code`, `message`, and `requestId`. Validation and configuration failures can throw ordinary errors.

An `outcome_unknown` operation may already have run and saved state. Retrying can run it twice; make retryable operations idempotent. `requestId` identifies an attempt, not a deduplication key.
