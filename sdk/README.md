# Durable Actors for TypeScript

Durable actors and typed clients, backed by the Rust runtime.

[Quickstart](#quickstart) · [Reference](https://github.com/TerseAI/durable-actors/blob/main/docs/reference/typescript.md) · [Runnable example](https://github.com/TerseAI/durable-actors/blob/main/examples/chat/README.md)

## Quickstart

Requires Node.js 22.19+, pnpm, and Bun 1.3.9+.

```sh
npx durable-actors init my-actors
cd my-actors
pnpm install
```

Define `src/actors.ts`. Fields marked `@Persisted` survive restarts. They share one SQLite database and transaction with `this.db` SQL. Litestream is installed and managed automatically with the runtime; local development persists to the filesystem.

```ts
import { Actor, Persisted } from "durable-actors"

export class Counter extends Actor {
    @Persisted private value = 0

    async increment() {
        return ++this.value
    }
}
```

```sh
pnpm exec durable-actors dev
```

## Call an actor

In another terminal in the same directory, generate the client:

```sh
pnpm exec durable-actors generate
```

Save as `client.ts` and run `bun client.ts`:

```ts
import { actors } from "./generated/index.js"

const counter = actors.Counter.get("one")
console.log(await counter.increment())
```

For WebSockets and browser integration, see the [chatroom example](https://github.com/TerseAI/durable-actors/blob/main/examples/chat/README.md).

# Defining an Actor

Every actor must extend the base `Actor` class in the sdk.

```ts
import { Actor } from "durable-actors"

export class Counter extends Actor {
    function foo(): String {
        return "bar"
    }
}
```

Amazing, it's an actor. Now, only 1 caller can invoke foo at a time. By itself, not revolutionary (yet).

Now lets add some properties.

```ts
import { Actor, Ephemeral } from "durable-actors"

export class Counter extends Actor {
    @Ephemeral private value = 0

    function change(value: Int): Int {
        this.value = value
        return this.value
    }
}
```

What is this `Ephemeral` thing though? This means that the property will **not** be durably persisted on every edit (the default for Durable Objects).

We now have a class that will only allow one request to change the value. If the actor goes idle, the value will reset to 0 when it comes back up.

Hm, ok. If we want this to track a bank balance let's say, pretty bad if it resets. We need this to persist.

```ts
import { Actor, Persisted } from "durable-actors"

export class Counter extends Actor {
    @Persisted private value = 0

    function change(value: Int): Int {
        this.value = value
        return this.value
    }
}
```

Now things are getting interesting, every time value is changed we are **durably storing** the new value. This happens real time and is very fast (88ms p95). Once that setter is done, the value is saved and can be recovered even if a meteor takes down a data center.

Ok what else can we store? Well, we actually give you a full SQLite database per instance here. The `Persisted` field is actually just a convenience.


