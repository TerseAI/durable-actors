from collections.abc import Callable
from dataclasses import MISSING, field
from typing import Any, TypeVar, overload

T = TypeVar("T")


@overload
def emitted(default: T) -> T: ...


@overload
def emitted(*, default_factory: Callable[[], T]) -> T: ...


def emitted(default: Any = MISSING, *, default_factory: Any = MISSING) -> Any:
    return field(
        default=default, default_factory=default_factory, metadata={"little_actors": "emitted"}
    )


@overload
def ephemeral(default: T) -> T: ...


@overload
def ephemeral(*, default_factory: Callable[[], T]) -> T: ...


def ephemeral(default: Any = MISSING, *, default_factory: Any = MISSING) -> Any:
    return field(
        default=default, default_factory=default_factory, metadata={"little_actors": "ephemeral"}
    )
