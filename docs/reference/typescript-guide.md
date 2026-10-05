# TypeScript guide

This guide edits `examples/bank-typescript`. Use that project as a basis if you are following along.

## Defining an actor

Each bank account is an actor. An actor has fields and methods. The @Persisted property decorator marks that a field should be saved after each successful method invocation.

For example, to track a bank balance, we create a `src/actors.ts` with the following actor definition:

```typescript
import { Actor, Persisted } from "durable-actors"

export class BankAccount extends Actor {
    @Persisted public balance = 0

    async getBalance(): Promise<number> {
        return this.balance
    }
}
```

## Generating a client

Start the local dev server:

```bash
cd examples/bank-typescript
durable-actors dev
```

And in a separate terminal generate the client:

```bash
cd examples/bank-typescript
durable-actors generate
```

This will create a generated folder, which contains stubs that you can use to reference your actors. Each actor can then be called by `id`. Create a `client.ts` and paste in the following:

```typescript
import { actors } from "./generated/index.js"

// reference by ID demo
const account = actors.BankAccount.get("demo")

// call method
console.log(await account.getBalance())
```

Actors get started on method invocation, continue in memory while performing operations and then scale back down after sitting idly.

## Ephemeral fields

So next, we want to track how many operations occur during the time an actor wakes and spins back down. This is pretty easy with the `@Ephemeral` decorator. The state will only be stored while the actor is awake and get wiped when it goes idle again. In `src/actors.ts`, you can update the actors definition to see how this works:

```typescript
import { Actor, Ephemeral, Persisted } from "durable-actors"

export class BankAccount extends Actor {
    @Persisted private balance = 0
    @Ephemeral private depositsSinceWake = 0

    async getBalance(): Promise<number> {
        return this.balance
    }
    async getDepositsSinceWake(): Promise<number> {
        return this.depositsSinceWake
    }
    async deposit(amount: number): Promise<number> {
        this.balance += amount
        this.depositsSinceWake += 1
        return this.balance
    }
}
```

## Websocket messages

durable-actors handles websocket messages for all connected clients. We have full type support for specifying socket metadata, incoming and outgoing messages sent via websocket. All payloads are JSON by default.

- Metadata. For each connected websocket, attach client metadata used to identify the user. In our example, that's userId, passed when the client connects.
- Incoming. The messages a client can send. In our example, { type: "ping" }. onMessage answers by sending the balance back to that socket.
- Outgoing. The messages the actor can send. In our example, { type: "balance", balance }, which both socket.send and broadcast use.

Additionally, we also support:

- Tags. setTags sets the tags on one socket. In onConnect, we tag it "customer".
- Auto-responses. A raw text ping gets pong back without waking the sandbox. We register that pair in onConnect. A JSON { type: "ping" } still wakes the actor, and onMessage runs.
- Broadcasts. broadcast sends to every connected client. deposit limits that to sockets tagged "customer". onDisconnect uses except to skip the socket that just left.
- Individual sends. socket.send sends to one socket. onConnect and onMessage use it to hand that client the current balance.

```typescript
import { Actor, type ActorSocket, Ephemeral, Persisted } from "durable-actors"

export class BankAccount extends Actor<Metadata, Incoming, Outgoing> {
    @Persisted private balance = 0
    @Ephemeral private depositsSinceWake = 0
    async getBalance(): Promise<number> {
        return this.balance
    }
    async getDepositsSinceWake(): Promise<number> {
        return this.depositsSinceWake
    }
    async deposit(amount: number): Promise<number> {
        this.balance += amount
        this.depositsSinceWake += 1
        this.broadcast({ type: "balance", balance: this.balance }, { tags: ["customer"] })
        return this.balance
    }
    async onConnect(socket: Socket): Promise<void> {
        socket.setTags("customer")
        this.setWebSocketAutoResponse({ request: "ping", response: "pong" })
        socket.send({ type: "balance", balance: this.balance })
    }
    async onMessage(socket: Socket): Promise<void> {
        socket.send({ type: "balance", balance: this.balance })
    }
    async onDisconnect(socket: Socket): Promise<void> {
        this.broadcast({ type: "balance", balance: this.balance }, { except: socket })
    }
}

type Metadata = { userId: string }
type Incoming = { type: "ping" }
type Outgoing = { type: "balance"; balance: number }
type Socket = ActorSocket<Metadata, Outgoing>
```

## Using emittable

`@Emittable` publishes public `@Persisted` field to every connected client.

- When a client connects, it receives a state message with the current emittable fields. In our example, that is message.state.balance.
- After a successful call changes the field, clients receive a state_update.

```typescript
import { Actor, type ActorSocket, Emittable, Ephemeral, Persisted } from "durable-actors"

export class BankAccount extends Actor<Metadata, Incoming, Outgoing> {
    @Persisted @Emittable balance = 0
    @Ephemeral private depositsSinceWake = 0
    async getBalance(): Promise<number> {
        return this.balance
    }
    async getDepositsSinceWake(): Promise<number> {
        return this.depositsSinceWake
    }
    async deposit(amount: number): Promise<number> {
        this.balance += amount
        this.depositsSinceWake += 1
        this.broadcast({ type: "balance", balance: this.balance }, { tags: ["customer"] })
        return this.balance
    }
    async onConnect(socket: Socket): Promise<void> {
        socket.setTags("customer")
        this.setWebSocketAutoResponse({ request: "ping", response: "pong" })
        socket.send({ type: "balance", balance: this.balance })
    }
    async onMessage(socket: Socket): Promise<void> {
        socket.send({ type: "balance", balance: this.balance })
    }
    async onDisconnect(socket: Socket): Promise<void> {
        this.broadcast({ type: "balance", balance: this.balance }, { except: socket })
    }
}
type Metadata = { userId: string }
type Incoming = { type: "ping" }
type Outgoing = { type: "balance"; balance: number }
type Socket = ActorSocket<Metadata, Outgoing>
```

And a connecting client would see:

```typescript
import { actors } from "./generated/index.js"

const account = actors.BankAccount.get("demo")
const socket = await account.connect({ userId: "ada" })
socket.addEventListener("message", event => {
    if (event.data.type === "state") console.log(event.data.state.balance)
    else if (event.data.type === "state_update") console.log(event.data.changes.balance)
    else console.log(event.data)
})
```

## Bringing in interleave

Actors run one call at a time, and a call holds the actor until it finishes, including across an await. @Interleave lets other calls run while that method is waiting.

```typescript
import { Actor, Emittable, Interleave, Persisted } from "durable-actors"

const clearingNetwork = {
    async submit(reference: string): Promise<string> {
        return `receipt:${reference}`
    }
}

export class Wire extends Actor {
    @Persisted @Emittable status = "requested"
    async getStatus(): Promise<string> {
        return this.status
    }
    @Interleave
    async submit(reference: string): Promise<string> {
        const receipt = await clearingNetwork.submit(reference)
        this.status = "submitted"
        return receipt
    }
}
```

`getStatus` can run while submit waits on the clearing network.

## Supporting SQLite

Each actor has an SQLite database available at `this.db`. `this.db.exec` executes one statement.

Writes commit automatically when the method call succeeds.

```typescript
import { Actor, Persisted } from "durable-actors"

export class BankAccount extends Actor {
    @Persisted private balance = 0
    async deposit(amount: number): Promise<number> {
        this.db.exec("CREATE TABLE IF NOT EXISTS ledger (amount INTEGER)")
        this.balance += amount
        this.db.exec("INSERT INTO ledger (amount) VALUES (?)", amount)
        return this.balance
    }
}
```

## Configuring actor resources

@Sandbox overrides the deployment defaults for this actor class.

Supported configurations include cpu, memory, idle timeout and regional placements.

```typescript
import { Actor, Emittable, Persisted, Sandbox } from "durable-actors"

@Sandbox({ cpu: 0.5, memoryMiB: 256, idleTimeoutMs: 60_000, regions: ["north-america-east"] })
export class BankAccount extends Actor {
    @Persisted @Emittable balance = 0
}
```
