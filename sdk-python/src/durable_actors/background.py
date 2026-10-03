from __future__ import annotations

import inspect
import json
from collections.abc import Callable, Generator
from contextlib import contextmanager
from contextvars import ContextVar
from copy import deepcopy
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .actor import Actor


@dataclass
class Scope:
    actor: object
    register: Callable[[Callable[[], Any], bool], None]
    active: bool = True


invocation: ContextVar[Scope] = ContextVar("actor_background_tasks")


def wait_until(actor: object, task: Callable[[], Any]) -> None:
    scope = invocation.get(None)
    if scope is None or scope.actor is not actor or not scope.active:
        raise RuntimeError("background tasks require an active actor invocation")
    if not callable(task) or inspect.iscoroutinefunction(task) or inspect.isgeneratorfunction(task):
        raise TypeError("wait_until requires a deferred callback")
    scope.register(task, False)


def run_task(
    actor: Actor[Any, Any, Any, Any], task: Callable[[Any], Any], value: Any, completion: str
) -> None:
    scope = invocation.get(None)
    if scope is None or scope.actor is not actor or not scope.active:
        raise RuntimeError("external tasks require an active actor invocation")
    if not any(
        isinstance(member, staticmethod) and member.__func__ is task
        for member in vars(type(actor)).values()
    ):
        raise TypeError("run_task requires a static method on the actor class")
    if inspect.iscoroutinefunction(task) or inspect.isgeneratorfunction(task):
        raise TypeError("Python external tasks must use synchronous def functions")
    from .contract import describe_actor

    if completion not in describe_actor(type(actor)).methods:
        raise ValueError("run_task requires a public completion method")
    value = json.loads(json.dumps(value, allow_nan=False))

    def work() -> dict[str, Any]:
        try:
            result = task(deepcopy(value))
            if inspect.isawaitable(result):
                raise TypeError("Python external tasks must use synchronous def functions")
            outcome = {
                "input": value,
                "ok": True,
                "value": json.loads(json.dumps(result, allow_nan=False)),
            }
        except Exception as error:
            outcome = {"input": value, "ok": False, "error": str(error)}
        return {"method": completion, "args": [outcome]}

    scope.register(work, True)


class BackgroundTasks:
    def __init__(self) -> None:
        self.sequence = 0
        self.pending: dict[int, Callable[[], Any]] = {}

    @contextmanager
    def scope(
        self, actor: object, interleaved: bool
    ) -> Generator[tuple[list[int], list[int]], None, None]:
        tasks: list[int] = []
        external_tasks: list[int] = []

        def register(task: Callable[[], Any], external: bool) -> None:
            if interleaved:
                raise RuntimeError("background tasks require a serialized actor")
            if len(self.pending) >= 64:
                raise RuntimeError("actor background task limit reached")
            self.sequence += 1
            self.pending[self.sequence] = task
            (external_tasks if external else tasks).append(self.sequence)

        scope = Scope(actor, register)
        token = invocation.set(scope)
        try:
            yield tasks, external_tasks
        except BaseException:
            self.discard(tasks + external_tasks)
            raise
        finally:
            scope.active = False
            invocation.reset(token)

    @property
    def has_pending(self) -> bool:
        return bool(self.pending)

    def take(self, task_id: Any) -> Callable[[], Any]:
        task = self.pending.pop(task_id, None) if type(task_id) is int else None
        if task is None:
            raise RuntimeError("background task is no longer available")
        return task

    def discard(self, tasks: list[int]) -> None:
        for task_id in tasks:
            self.pending.pop(task_id, None)

    def clear(self) -> None:
        self.pending.clear()
