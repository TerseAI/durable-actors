"""UTC cron schedules and their durable occurrence identity."""

import inspect
from collections.abc import Callable
from typing import Any, TypeVar, cast, get_type_hints

from pydantic import BaseModel, ConfigDict, Field

F = TypeVar("F", bound=Callable[..., Any])


class CronEvent(BaseModel):
    """The original schedule occurrence, unchanged across retries.

    scheduled_time is Unix time in milliseconds, in UTC.
    """

    model_config = ConfigDict(frozen=True, populate_by_name=True)
    cron: str
    scheduled_time: int = Field(alias="scheduledTime")


def cron(expression: str, *, retries: int = 0) -> Callable[[F], F]:
    """Run in UTC. Handler retries default to zero; delivery recovery is automatic."""
    validate_expression(expression)
    if type(retries) is not int or not 0 <= retries <= 2**31 - 1:
        raise ValueError("cron retries must be a nonnegative 32-bit integer")

    def decorate(method: F) -> F:
        if not inspect.isfunction(method) or method.__name__.startswith("_"):
            raise ValueError("cron requires a public instance method")
        schedules = getattr(method, "__actor_crons__", ())
        if any(schedule[0] == expression for schedule in schedules):
            raise ValueError("duplicate cron schedule")
        setattr(method, "__actor_crons__", (*schedules, (expression, retries)))
        return cast(F, method)

    return decorate


def cron_contract(actor: type) -> dict[str, Any]:
    schedules: list[dict[str, Any]] = []
    for name, method in vars(actor).items():
        expressions = getattr(method, "__actor_crons__", ())
        if isinstance(method, (staticmethod, classmethod)):
            if getattr(getattr(cast(Any, method), "__func__"), "__actor_crons__", ()):
                raise ValueError("cron requires a public instance method")
            continue
        if not expressions:
            continue
        parameters = list(inspect.signature(method).parameters.values())
        hints = get_type_hints(method)
        if (
            name in {"on_connect", "on_message", "on_disconnect"}
            or len(parameters) != 2
            or hints.get(parameters[-1].name) is not CronEvent
            or hints.get("return") is not type(None)
            or parameters[-1].default is not inspect.Parameter.empty
            or parameters[-1].kind
            not in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
        ):
            raise ValueError("cron requires one CronEvent parameter and a None result")
        schedules.extend(
            {"method": name, "expression": value, **({"retries": retries} if retries else {})}
            for value, retries in expressions
        )
    schedules.sort(key=lambda value: (value["method"], value["expression"]))
    return {"crons": schedules} if schedules else {}


def validate_expression(expression: object) -> None:
    if not isinstance(expression, str) or len(expression.split()) != 5:
        raise ValueError("cron requires a five-field cron expression")
