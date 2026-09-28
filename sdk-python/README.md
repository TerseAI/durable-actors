# Little Actors for Python

Define durable actors in Python and generate typed Python clients. Python 3.11+ is supported. Actor state, placement, routing, and WebSocket delivery use the same Rust runtime as the TypeScript SDK.

```sh
pnpm dlx durable-actors init my-actors --template python
cd my-actors
pnpm install
uv sync
pnpm exec durable-actors dev
```

The shared TypeScript CLI manages development, generation, and Python type checking. It selects the project's `.venv` (or the active environment); `DURABLE_ACTORS_PYTHON` overrides the interpreter. Install Node.js 22+, Python 3.11+, and uv. The CLI downloads the matching native runtime on macOS and Linux. When developing this repository, set `DURABLE_ACTORS_BINARY` to an absolute path to your `cargo build --locked` executable.

## Define actors

```python
from pydantic import BaseModel
from little_actors import Actor, emitted, ephemeral

class Message(BaseModel):
    text: str

class Chat(Actor):
    count: int = 0
    messages: list[Message] = emitted(default_factory=list)
    busy: bool = ephemeral(False)

    async def append(self, message: Message) -> list[Message]:
        self.messages.append(message)
        return self.messages
```

Public async methods become RPCs. Annotated fields persist by default. `emitted()` persists a field and broadcasts its saved changes; `ephemeral()` keeps a field temporary. Mutable defaults are copied for each actor, and both helpers accept `default_factory` for values constructed on activation. Use `ephemeral(default_factory=...)` for locks, caches, and service clients, and `ClassVar` for class constants. Classes extend `Actor` directly and use field defaults instead of constructors. Prefix helper methods with `_`.

## Generate and use a client

With the actor server running, generate clients in your application:

```sh
pnpm add -D durable-actors
uv add 'little-actors[codegen]'
pnpm exec durable-actors generate --out-dir generated
```

You can also generate directly from a trusted source entrypoint:

```sh
pnpm exec durable-actors generate src/actors.py --out-dir generated
```

```python
from little_actors import Client
from generated import Chat
from generated.chat_models import Message

with Client() as transport:
    chat = Chat("lobby", transport)
    messages: list[Message] = chat.append(Message(text="hello"))
    print(messages[0].text)
```

The CLI runs strict mypy on local actor definitions before generation and on the generated package afterward. `dev` checks definitions before startup and every reload; an invalid edit leaves the previous code running.

The client uses synchronous calls and context managers. Generated RPC methods return typed values directly.

The generated package includes method signatures, independent Pydantic models, and `py.typed`. Consumers need only `little-actors`, not the actor project or the code generator. Regenerate after changing the actor contract, and include the generated package in your application's type checks.

Typing uses inline annotations and the [PEP 561](https://peps.python.org/pep-0561/) package marker. Both mypy and Pyright check the SDK and generated clients. Pydantic validates inputs, outputs, and persisted state at runtime. Python annotations remain ordinary annotations: `chat.append(42)` is rejected by a type checker and by runtime validation.

See the [Python reference](https://github.com/TerseAI/durable-actors/blob/main/docs/reference/python.md) for supported types, sockets, reentrancy, deployment, and CLI options.
