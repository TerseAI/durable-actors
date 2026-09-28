# Python SDK

The `little-actors` distribution provides actor authoring, a Python executor, and async clients, driven by the shared TypeScript `durable-actors` CLI. Install `little-actors[codegen]` in development environments for generation and type checking. See the [quickstart](../../sdk-python/README.md) and [runnable example](../../examples/python).

## Actor definitions and typing

```python
from typing import Annotated, Literal
from pydantic import BaseModel, Field
from little_actors import Actor, Persisted, Ephemeral, Emittable

class Message(BaseModel):
    role: Literal["user", "assistant"]
    text: Annotated[str, Field(min_length=1)]

class Chat(Actor):
    messages: Annotated[list[Message], Persisted(), Emittable()] = []
    busy: Annotated[bool, Ephemeral()] = False

    async def append(self, message: Message) -> list[Message]:
        self.messages.append(message)
        return self.messages
```

Every RPC parameter, return value, and persisted field needs a concrete annotation. Supported schema types include JSON primitives, typed collections with string dictionary keys, fixed tuples, optional values, literals, unions (including discriminated unions), recursive models, Pydantic models, dataclasses, `typing_extensions.TypedDict`, dates, datetimes, and UUIDs. `JsonValue` explicitly describes arbitrary nested JSON. Bare collections, `Any`, and `object` are rejected at public boundaries. Ephemeral fields can hold Python objects such as API clients and locks.

Source annotations produce JSON Schema. Generated clients have real async methods and independent Pydantic models that describe the wire representation. Import those models from `generated.<actor_name>_models`; actor implementation classes and source-only dependencies are not needed by consumers. Schema constraints are generated into client models. When serialization changes a model shape, separate `Input` and `Output` models describe the two wire types; computed fields appear in outputs. Source-only Python validators and methods remain on the actor side, so the actor validates every call as well.

Inline annotations and a `py.typed` marker support mypy and Pyright without a custom checker plugin. Tests verify both valid calls and rejection of invalid calls, including generated model return types. Runtime validation is strict: a string is not coerced into an integer RPC argument.

Classes extend `Actor` directly. Public async methods are RPCs; `_` methods are helpers. Properties and static methods are not RPCs. `get_connections`, `broadcast`, `connect`, `prepare_websocket`, and `get` are reserved. Lifecycle hooks are described below. Positional, positional-only, keyword-only, defaulted, and final variadic parameters are supported. The wire contract requires required parameters before optional parameters and variadic parameters last; `**kwargs` is unsupported.

Each field must have a default or a `default_factory` on its `Persisted` or `Ephemeral` annotation. Defaults are copied per instance. Factories run when an actor activates, rather than during schema extraction. Actor constructors are not supported. For example:

```python
from asyncio import Lock
lock: Annotated[Lock, Ephemeral(default_factory=Lock)]
```

Persisted fields beginning with `_` stay out of the public socket state schema. They still exist in persisted storage and administrative state inspection. `Emittable()` requires a public persisted field and broadcasts its saved changes to connected clients. Undeclared instance fields are rejected when saving state.

## Execution and failures

Methods run serially by default. A failed invocation restores persisted state to its previous snapshot. Ephemeral defaults are recreated when restoring or reactivating an actor. Use `self.id` within methods and hooks to read the current actor ID.

`@reentrant` permits other calls to enter while a method awaits. Reentrant actors share a live Python instance; their mutations are not rolled back on exceptions, since doing so would overwrite overlapping successful work. Completion sequences preserve commit ordering in Rust. Use reentrancy deliberately for streaming or long waits. Move blocking work off the event loop with `asyncio.to_thread`.

The async HTTP client caches direct actor routes, refreshes stale routes, and retries only a rejection known to precede execution. `ActorInvocationError` exposes `code` and `request_id`. A lost response raises `outcome_unknown`; automatically replaying it could repeat actor side effects.

## Typed WebSockets

Declare `Actor[Metadata, Incoming, Outgoing]` to type both ends of the socket. The defaults are `JsonValue`.

```python
class Room(Actor[Member, Message, Message]):
    async def on_connect(self, socket: ActorSocket[Member, Message]) -> None:
        socket.set_tags("members")

    async def on_message(self, socket: ActorSocket[Member, Message], message: Message) -> None:
        self.broadcast(message)

    async def on_disconnect(
        self, socket: ActorSocket[Member, Message], code: int,
        reason: str, was_clean: bool,
    ) -> None:
        pass
```

Import `ActorSocket` from `little_actors`. `await self.get_connections()` returns typed sockets. Each socket has `id`, `metadata`, `tags`, and `state`, with `send`, `close`, `reject`, and `set_tags` operations. Assign `socket.metadata` to update it. `reject()` defaults to application close code 4003 and is valid only during connection. Socket handles are scoped to the active invocation.

```python
from generated import Room
from generated.room_models import Member, Message
from little_actors import Client, StateSnapshot, StateUpdate

async with Client() as transport:
    room = Room("lobby", transport)
    async with await room.connect(Member(name="Ada")) as connection:
        await connection.send(Message(role="user", text="hello"))
        async for event in connection:
            if isinstance(event, StateSnapshot):
                print(event.state)
            elif isinstance(event, StateUpdate):
                print(event.changes.model_dump(exclude_unset=True), event.removed)
            else:
                print(event.text)
```

`StateSnapshot` contains all emittable fields; `StateUpdate.changes` contains changed fields. Optional fields without a schema default use the typed `UNSET` sentinel, exported from `little_actors`. This preserves the distinction between omission and an explicit `None`. Use `model_fields_set`, `isinstance(value, Unset)`, or `model_dump(exclude_unset=True)` when processing patches. `prepare_websocket(metadata)` returns a short-lived grant when another process will connect. Reconnect explicitly with a new grant after expiry or connection loss.

Metadata is limited to 64 KiB and messages to 16 MiB. Application messages cannot use the reserved top-level types `state` or `state_update`.

## CLI and configuration

| Command | Purpose |
| --- | --- |
| `durable-actors init DIRECTORY --template python` | Create a typed counter project with Python and Node manifests. |
| `durable-actors dev` | Type-check and run actors; check and reload Python edits. |
| `durable-actors dev --no-watch` | Run without source watching. |
| `durable-actors generate [src/actors.py] --out-dir generated` | Generate from source, or the server when source is omitted, then type-check the client. |
| `durable-actors generate --language python` | Explicitly select Python clients for a published schema. |

Use the existing Node CLI (`pnpm exec durable-actors`). The Python package has no console command. Install `little-actors[codegen]` in your project's virtual environment. The CLI runs strict mypy before source compilation and reload, and after generation. Invalid edits leave the last working deployment running. The SDK and generated-client tests also validate Pyright compatibility.

The CLI selects `DURABLE_ACTORS_PYTHON`, then the active `VIRTUAL_ENV`, then the project's `.venv/bin/python`, then `python3`. Set `DURABLE_ACTORS_ENTRYPOINT=src/actors.py` in `.env`; the Python template creates this setting. `.env.local` and `.env` work just as they do for TypeScript projects. `DURABLE_ACTORS_PROJECT` overrides the project directory, and `DURABLE_ACTORS_DATA_DIR` the local state directory. `dev --port 0` selects a free port. `DURABLE_ACTORS_BINARY` selects a native executable instead of downloading it. Keep native runtime, TypeScript CLI, and Python SDK versions aligned.

Client constructor arguments override environment variables. Set `DURABLE_ACTORS_CONTROL_PLANE_URL`, `DURABLE_ACTORS_PROJECT_ID`, `DURABLE_ACTORS_SECRET`, and optionally `DURABLE_ACTORS_HOME_REGION`. Local defaults are `http://127.0.0.1:7100` and project `local`. Remote origins require a project ID. An injected `httpx.AsyncClient` lets applications supply transport and timeout policy; its lifecycle remains with the application. CLI commands load `.env.local` and `.env`, preserving exported environment overrides.

Generation from local source imports that source. Generation from a server consumes schemas and does not execute the actor implementation.

## Deployment

The runtime image includes Python 3.13 and this SDK. Register a hosted deployment through the [HTTP API](openapi.md), using the source image's project directory and a Python `actorEntrypoint`, such as `actors.py`. The Modal builder installs dependencies, extracts the contract, and snapshots `actors.pyz` and its sibling `python/` dependency directory. Python hosts implement the existing executor protocol, persistence, residency, and socket effects. Spare sandboxes switch to a Python executor upon assignment; Python itself is not prewarmed in the shared Bun spare pool.

Build dependencies come from `requirements.txt` when present, otherwise from `[project].dependencies` in `pyproject.toml`. Pin dependencies for reproducible builds. The SDK dependency in `pyproject.toml` must match the runtime's installed SDK. The hosted Python version and platform must support any native dependencies; build them in the runtime image, rather than copying a macOS virtual environment into a Linux deployment.

Python source is packaged from the project, excluding hidden directories, `node_modules`, `venv`, `__pycache__`, `dist`, `target`, and `generated`. Export actor classes from the entrypoint; an optional `__all__` controls exports. Explicitly include resources as needed:

```toml
[tool.little-actors]
include = ["templates/*.txt", "data/*.json"]
```

Use `importlib.resources` for packaged resources. Source and resources are limited to 32 MiB; dependency packages live outside the source archive. Each executor is permanently assigned to one actor identity. The generated clients in this SDK target Python.

## Releases

Python package versions are stamped and verified with the native and TypeScript versions by `scripts/release.mjs`. CI runs Python 3.11, 3.13, and 3.14, checks mypy and Pyright, and tests against a built Rust runtime. The release workflow publishes the tested wheel and source distribution through PyPI trusted publishing after native and image validation.

Before the first publication, the package owner must configure the `little-actors` PyPI project (or a pending publisher) for GitHub owner `TerseAI`, repository `durable-actors`, workflow `release.yml`, environment `pypi`. See [PyPI's trusted publishing setup](https://docs.pypi.org/trusted-publishers/adding-a-publisher/).
