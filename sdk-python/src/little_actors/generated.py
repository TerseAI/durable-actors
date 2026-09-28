from typing import Any

from pydantic import BaseModel, ConfigDict, TypeAdapter

from .contract import encode


class Unset:
    pass


UNSET = Unset()


class ClientModel(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)


def is_unset(value: object) -> bool:
    return isinstance(value, Unset)


def argument(value: Any, adapter: TypeAdapter[Any]) -> Any:
    return value if isinstance(value, Unset) else encode(adapter, value)


def arguments(values: list[Any], defaults: list[Any]) -> list[Any]:
    while values and isinstance(values[-1], Unset):
        values.pop()
    return [
        defaults[index] if isinstance(value, Unset) else value for index, value in enumerate(values)
    ]
