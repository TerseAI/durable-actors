import importlib
import json
import subprocess
import sys

from test_authoring import Chat

from little_actors.codegen import generate_client
from little_actors.contract import public_contract


class Transport:
    async def prepare_websocket(
        self, actor_name, actor_id, metadata, *, authorization_lifetime_ms=900000
    ):
        raise AssertionError("not used")

    async def invoke(self, actor_name, actor_id, method, args):
        assert (actor_name, actor_id, method) == ("Chat", "lobby", "append")
        return args


async def test_generated_client_returns_typed_models_without_actor_source(tmp_path, monkeypatch):
    package = tmp_path / "generated"
    generate_client(json.loads(json.dumps(public_contract([Chat]))), package)
    monkeypatch.syspath_prepend(str(tmp_path))
    module = importlib.import_module("generated")
    models = importlib.import_module("generated.chat_models")
    message = models.Message(text="hello", role="user")
    result = await module.Chat("lobby", Transport()).append(message)
    assert isinstance(result[0], models.Message)
    assert result[0].text == "hello"
    assert (package / "py.typed").is_file()
    source = tmp_path / "usage.py"
    source.write_text("""from generated import Chat
from generated.chat_models import Message
from little_actors.client import Client
async def check(client: Client) -> None:
    chat = Chat("lobby", client)
    result: list[Message] = await chat.append(Message(text="hello", role="user"))
    await chat.append(42)
    connection = await chat.connect(Message(text="hello", role="user"))
    await connection.send(42)
""")
    for checker in ("mypy", "pyright"):
        result = subprocess.run(
            [sys.executable, "-m", checker, str(source)],
            capture_output=True,
            text=True,
            cwd=tmp_path,
        )
        assert result.returncode == 1, result.stdout + result.stderr
        assert "42" in result.stdout or "int" in result.stdout
        assert "2 errors" in result.stdout, result.stdout


async def test_generated_rest_and_keyword_parameters_preserve_calling_convention(
    tmp_path, monkeypatch
):
    from little_actors import Actor

    class Parameters(Actor):
        async def total(self, initial: int, *values: int) -> int:
            return initial + sum(values)

        async def label(self, *, value: str = "default") -> str:
            return value

    generate_client(public_contract([Parameters]), tmp_path / "parameters_client")
    monkeypatch.syspath_prepend(str(tmp_path))
    generated = importlib.import_module("parameters_client")

    class Calls:
        async def prepare_websocket(
            self, actor_name, actor_id, metadata, *, authorization_lifetime_ms=900000
        ):
            raise AssertionError("not used")

        async def invoke(self, actor_name, actor_id, method, args):
            return sum(args) if method == "total" else (args[0] if args else "default")

    client = generated.Parameters("one", Calls())
    assert await client.total(1, 2, 3) == 6
    assert await client.label(value="hello") == "hello"
    assert await client.label() == "default"


async def test_generated_names_cannot_shadow_client_runtime(tmp_path, monkeypatch):
    from little_actors import Actor

    class Connection(Actor):
        async def call(self, json: str, TypeAdapter: int, argument: bool) -> str:
            return json

    generate_client(public_contract([Connection]), tmp_path / "names_client")
    monkeypatch.syspath_prepend(str(tmp_path))
    module = importlib.import_module("names_client")

    class Calls:
        async def prepare_websocket(
            self, actor_name, actor_id, metadata, *, authorization_lifetime_ms=900000
        ):
            raise AssertionError("not used")

        async def invoke(self, actor_name, actor_id, method, args):
            return args[0]

    assert await module.Connection("one", Calls()).call("value", 42, True) == "value"


async def test_generated_recursive_unions_dates_and_tuples(tmp_path, monkeypatch):
    from datetime import datetime, timezone
    from uuid import uuid4

    from fixtures.effects import Effects
    from fixtures.types import Trees

    from little_actors.runtime import ActorRuntime

    generate_client(public_contract([Trees]), tmp_path / "tree_client")
    monkeypatch.syspath_prepend(str(tmp_path))
    remote = importlib.import_module("tree_client")
    models = importlib.import_module("tree_client.trees_models")
    runtime = ActorRuntime(Trees, Effects())

    class Calls:
        async def prepare_websocket(
            self, actor_name, actor_id, metadata, *, authorization_lifetime_ms=900000
        ):
            raise AssertionError("not used")

        async def invoke(self, actor_name, actor_id, method, args):
            reply = await runtime.handle(
                {
                    "type": "invoke",
                    "request_id": "one",
                    "actor": {
                        "project_id": "local",
                        "actor_name": actor_name,
                        "actor_id": actor_id,
                    },
                    "state": None,
                    "method": method,
                    "args": args,
                }
            )
            assert reply["type"] == "invoked", reply
            return reply["result"]

    now, identity = datetime.now(timezone.utc), uuid4()
    node = models.NodeInput(
        item=models.BranchInput(
            children=[
                models.NodeInput(item=models.Leaf(value=1), created_at=now, identity=identity)
            ]
        ),
        created_at=now,
        identity=identity,
    )
    client = remote.Trees("one", Calls())
    result = await client.append(node)
    assert result[0].item.children[0].item.value == 1
    assert result[0].created_at == now
    assert result[0].identity == identity
    assert await client.pair((7, "seven")) == (7, "seven")
    source = tmp_path / "typed_tree.py"
    source.write_text("""from tree_client import Trees
from tree_client.trees_models import NodeInput, NodeOutput
from little_actors import Client
async def check(client: Client, node: NodeInput) -> None:
    tree = Trees("one", client)
    nodes: list[NodeOutput] = await tree.append(node)
    pair: tuple[int, str] = await tree.pair((1, "one"))
""")
    for checker in ("mypy", "pyright"):
        result = subprocess.run(
            [sys.executable, "-m", checker, str(source)],
            cwd=tmp_path,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stdout + result.stderr


async def test_generated_models_preserve_omitted_typed_dict_fields(tmp_path, monkeypatch):
    from fixtures.effects import Effects
    from fixtures.types import OptionActor

    from little_actors.runtime import ActorRuntime

    runtime = ActorRuntime(OptionActor, Effects())
    generate_client(public_contract([OptionActor]), tmp_path / "options_client")
    monkeypatch.syspath_prepend(str(tmp_path))
    remote = importlib.import_module("options_client")
    models = importlib.import_module("options_client.optionactor_models")

    class Calls:
        async def prepare_websocket(
            self, actor_name, actor_id, metadata, *, authorization_lifetime_ms=900000
        ):
            raise AssertionError("not used")

        async def invoke(self, actor_name, actor_id, method, args):
            reply = await runtime.handle(
                {
                    "type": "invoke",
                    "actor": {
                        "project_id": "local",
                        "actor_name": actor_name,
                        "actor_id": actor_id,
                    },
                    "state": None,
                    "method": method,
                    "args": args,
                }
            )
            assert reply["type"] == "invoked", reply
            return reply["result"]

    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        models.Options(required="present", optional=None)
    result = await remote.OptionActor("one", Calls()).echo(models.Options(required="present"))
    assert result.model_dump(exclude_unset=True) == {"required": "present"}
