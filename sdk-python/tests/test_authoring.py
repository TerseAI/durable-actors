from typing import Annotated, Literal

import pytest
from pydantic import BaseModel

from little_actors import Actor, Emittable, Ephemeral, Persisted, reentrant
from little_actors.contract import describe_actor, public_contract


class Message(BaseModel):
    text: str
    role: Literal["user", "assistant"]


class Chat(Actor[Message, Message, Message]):
    messages: Annotated[list[Message], Persisted(), Emittable()] = []
    busy: Annotated[bool, Ephemeral()] = False

    async def append(self, message: Message) -> list[Message]:
        self.messages.append(message)
        return self.messages

    async def clear(self) -> None:
        self.messages.clear()


def test_contract_describes_models_methods_and_public_state():
    contract = public_contract([Chat])
    actor = contract["actors"][0]
    assert contract["version"] == 1
    assert actor["actorName"] == "Chat"
    assert [method["name"] for method in actor["rpc"]["methods"]] == ["append", "clear"]
    assert actor["rpc"]["methods"][1]["result"] == {"kind": "void"}
    assert actor["socket"]["emittable"] == ["messages"]
    assert list(actor["socket"]["schema"]["definitions"]["State"]["properties"]) == ["messages"]
    assert "Message" in actor["rpc"]["schema"]["definitions"]


def test_mutable_defaults_are_per_actor():
    left, right = Chat(), Chat()
    left.messages.append(Message(text="hello", role="user"))
    assert right.messages == []


def test_missing_public_annotations_are_rejected():
    class Bad(Actor):
        async def echo(self, value):
            return value

    with pytest.raises(ValueError, match="annotation"):
        describe_actor(Bad)


def test_fields_require_persistence_annotations():
    class Bad(Actor):
        count: int = 0

    with pytest.raises(ValueError, match="Persisted|Ephemeral"):
        describe_actor(Bad)


def test_reentrant_preserves_method_types_and_marks_runtime_metadata():
    class Counter(Actor):
        @reentrant
        async def read(self) -> int:
            return 1

    assert describe_actor(Counter).reentrant_methods == {"read"}


def test_nested_untyped_values_are_rejected():
    from typing import Any

    class Bad(Actor):
        async def echo(self, value: list[Any]) -> int:
            return 1

    with pytest.raises(ValueError, match="concrete|Any"):
        describe_actor(Bad)


def test_nested_untyped_structural_models_are_rejected():
    from dataclasses import make_dataclass
    from typing import Any

    from little_actors.contract import adapter_for

    for value in (make_dataclass("Untyped", [("payload", Any)]),):
        with pytest.raises(ValueError, match="concrete"):
            adapter_for(value)


def test_untyped_aliases_and_non_string_dictionary_keys_are_rejected():
    from typing import Any

    from typing_extensions import TypeAliasType

    from little_actors.contract import adapter_for

    for value in (TypeAliasType("Untyped", list[Any]), dict[int, str]):
        with pytest.raises(ValueError, match="concrete|string"):
            adapter_for(value)


def test_variadic_rpc_parameters_must_be_last():
    class Bad(Actor):
        async def echo(self, *values: int, label: str = "") -> int:
            return len(values)

    with pytest.raises(ValueError, match="variadic"):
        describe_actor(Bad)


def test_field_factories_construct_fresh_ephemeral_resources():
    from threading import Lock

    class Guarded(Actor):
        lock: Annotated[object, Ephemeral(default_factory=Lock)]

    left, right = Guarded(), Guarded()
    assert left.lock is not right.lock
    assert hasattr(left.lock, "acquire")


def test_socket_model_names_may_match_contract_root_names():
    from jsonschema import Draft7Validator

    class Metadata(BaseModel):
        member: str

    class Incoming(BaseModel):
        text: str

    class Room(Actor[Metadata, Incoming, Incoming]):
        pass

    contract = public_contract([Room])["actors"][0]["socket"]["schema"]
    Draft7Validator({**contract, "$ref": "#/definitions/Metadata"}).validate({"member": "one"})
    Draft7Validator({**contract, "$ref": "#/definitions/Incoming"}).validate({"text": "hello"})


def test_nonfinite_json_and_unbound_type_variables_are_rejected():
    from typing import TypeVar

    from little_actors.contract import adapter_for, encode

    with pytest.raises(ValueError, match="concrete"):
        adapter_for(TypeVar("Unknown"))
    with pytest.raises(ValueError):
        encode(adapter_for(float), float("nan"))


def test_contract_uses_serialized_types_for_results():
    from pydantic import computed_field

    from little_actors.contract import adapter_for, schemas

    class Result(BaseModel):
        value: int

        @computed_field
        @property
        def doubled(self) -> int:
            return self.value * 2

    contract = schemas({"Result": adapter_for(Result)}, serialization={"Result"})
    result = contract["definitions"]["Result"]
    while "$ref" in result:
        result = contract["definitions"][result["$ref"].split("/")[-1]]
    assert result["properties"]["doubled"]["type"] == "integer"
