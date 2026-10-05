# Python guide

This guide edits `examples/bank-python`. Use that project as a basis if you are following along.

## Defining an actor

Each bank account is an actor. An actor has fields and methods. persisted() marks that a field should be saved after each successful method invocation.

For example, to track a bank balance, we create a `src/actors.py` with the following actor definition:

```python
from durable_actors import Actor, persisted


class BankAccount(Actor):
    _balance: int = persisted(0)

    def get_balance(self) -> int:
        return self._balance
```

## Generating a client

Install the project's virtual environment and start the local dev server:

```bash
cd examples/bank-python
uv sync
durable-actors dev
```

And in a separate terminal generate the client:

```bash
cd examples/bank-python
durable-actors generate
```

This will create a generated folder, which contains stubs that you can use to reference your actors. Each actor can then be called by `id`. We will discuss more on accessing actors but referencing an actor and then calling a method looks like:

```python
from generated import actors

# reference by ID demo
account = actors.BankAccount.get("demo")

# call method
print(account.get_balance())
```

The example's `client.py` does this. Run it with the `.env` printed by `durable-actors dev` loaded:

```bash
uv run --env-file .env client.py
```

Actors get started on method invocation, continue in memory while performing operations and then scale back down after sitting idly.

### Ephemeral fields

So next, we want to track how many operations occur during the time an actor wakes and spins back down. This is pretty easy with `ephemeral()`. The state will only be stored while the actor is awake and get wiped when it goes idle again.

```python
from durable_actors import Actor, ephemeral, persisted


class BankAccount(Actor):
    _balance: int = persisted(0)
    _deposits_since_wake: int = ephemeral(0)

    def get_balance(self) -> int:
        return self._balance

    def get_deposits_since_wake(self) -> int:
        return self._deposits_since_wake

    def deposit(self, amount: int) -> int:
        self._balance += amount
        self._deposits_since_wake += 1
        return self._balance
```

### Websocket messages

durable-actors handles websocket messages for all connected clients. We have full type support for specifying socket metadata, incoming and outgoing messages sent via websocket. All payloads are JSON by default.

- Metadata. For each connected websocket, attach client metadata used to identify the user. In our example, that's user_id, passed when the client connects.
- Incoming. The messages a client can send. In our example, { type: "ping" }. on_message answers by sending the balance back to that socket.
- Outgoing. The messages the actor can send. In our example, { type: "balance", balance }, which both socket.send and broadcast use.

Additionally, we also support:

- Tags. set_tags sets the tags on one socket. In on_connect, we tag it "customer".
- Auto-responses. A raw text ping gets pong back without waking the sandbox. We register that pair in on_connect. A JSON { type: "ping" } still wakes the actor, and on_message runs.
- Broadcasts. broadcast sends to every connected client. deposit limits that to sockets tagged "customer". on_disconnect uses except_ids to skip the socket that just left.
- Individual sends. socket.send sends to one socket. on_connect and on_message use it to hand that client the current balance.

```python
from typing import Literal

from durable_actors import Actor, ActorSocket, ephemeral, persisted
from pydantic import BaseModel


class Metadata(BaseModel):
    user_id: str


class Incoming(BaseModel):
    type: Literal["ping"]


class Outgoing(BaseModel):
    type: Literal["balance"]
    balance: int


Socket = ActorSocket[Metadata, Outgoing]


class BankAccount(Actor[Metadata, Incoming, Outgoing]):
    _balance: int = persisted(0)
    _deposits_since_wake: int = ephemeral(0)

    def get_balance(self) -> int:
        return self._balance

    def get_deposits_since_wake(self) -> int:
        return self._deposits_since_wake

    def deposit(self, amount: int) -> int:
        self._balance += amount
        self._deposits_since_wake += 1
        self.broadcast(Outgoing(type="balance", balance=self._balance), tags=("customer",))
        return self._balance

    def on_connect(self, socket: Socket) -> None:
        socket.set_tags("customer")
        self.set_websocket_auto_response("ping", "pong")
        socket.send(Outgoing(type="balance", balance=self._balance))

    def on_message(self, socket: Socket, message: Incoming) -> None:
        socket.send(Outgoing(type="balance", balance=self._balance))

    def on_disconnect(self, socket: Socket, code: int, reason: str, was_clean: bool) -> None:
        self.broadcast(Outgoing(type="balance", balance=self._balance), except_ids=(socket.id,))
```

### Using emittable

`emitted()` publishes a public `persisted()` field to every connected client.

- When a client connects, it receives a state message with the current emittable fields. In our example, that is message.state.balance.
- After a successful call changes the field, clients receive a state_update.

```python
from typing import Literal

from durable_actors import Actor, ActorSocket, emitted, ephemeral, persisted
from pydantic import BaseModel


class Metadata(BaseModel):
    user_id: str


class Incoming(BaseModel):
    type: Literal["ping"]


class Outgoing(BaseModel):
    type: Literal["balance"]
    balance: int


Socket = ActorSocket[Metadata, Outgoing]


class BankAccount(Actor[Metadata, Incoming, Outgoing]):
    balance: int = emitted(persisted(0))
    _deposits_since_wake: int = ephemeral(0)

    def get_balance(self) -> int:
        return self.balance

    def get_deposits_since_wake(self) -> int:
        return self._deposits_since_wake

    def deposit(self, amount: int) -> int:
        self.balance += amount
        self._deposits_since_wake += 1
        self.broadcast(Outgoing(type="balance", balance=self.balance), tags=("customer",))
        return self.balance

    def on_connect(self, socket: Socket) -> None:
        socket.set_tags("customer")
        self.set_websocket_auto_response("ping", "pong")
        socket.send(Outgoing(type="balance", balance=self.balance))

    def on_message(self, socket: Socket, message: Incoming) -> None:
        socket.send(Outgoing(type="balance", balance=self.balance))

    def on_disconnect(self, socket: Socket, code: int, reason: str, was_clean: bool) -> None:
        self.broadcast(Outgoing(type="balance", balance=self.balance), except_ids=(socket.id,))
```

And a connecting client would see:

```python
from durable_actors import StateSnapshot, StateUpdate
from generated import actors

account = actors.BankAccount.get("demo")
with account.connect(actors.BankAccount.Metadata(user_id="ada")) as socket:
    for message in socket:
        if isinstance(message, StateSnapshot):
            print(message.state.balance)
        elif isinstance(message, StateUpdate):
            print(message.changes.balance)
        else:
            print(message)
```

### Bringing in interleave

Actors run one call at a time, and a call holds the actor until it finishes, including while it waits. @interleave lets other calls run while that method is waiting.

```python
from durable_actors import Actor, emitted, interleave, persisted


class ClearingNetwork:
    def submit(self, reference: str) -> str:
        return f"receipt:{reference}"


clearing_network = ClearingNetwork()


class Wire(Actor):
    status: str = emitted(persisted("requested"))

    def get_status(self) -> str:
        return self.status

    @interleave
    def submit(self, reference: str) -> str:
        receipt = clearing_network.submit(reference)
        self.status = "submitted"
        return receipt
```

`get_status` can run while submit waits on the clearing network.

### Supporting SQLite

Each actor has an SQLite database available at `self.db`. `self.db.exec` executes one statement.

Writes commit automatically when the method call succeeds.

```python
from durable_actors import Actor, persisted


class BankAccount(Actor):
    _balance: int = persisted(0)

    def deposit(self, amount: int) -> int:
        self.db.exec("CREATE TABLE IF NOT EXISTS ledger (amount INTEGER)")
        self._balance += amount
        self.db.exec("INSERT INTO ledger (amount) VALUES (?)", amount)
        return self._balance
```

## Configuring actor resources

@sandbox overrides the deployment defaults for this actor class.

Supported configurations include cpu, memory, idle timeout and regional placements.

```python
from durable_actors import Actor, emitted, persisted, sandbox


@sandbox(cpu=0.5, memory_mib=256, idle_timeout_ms=60_000, regions=["north-america-east"])
class BankAccount(Actor):
    balance: int = emitted(persisted(0))
```
