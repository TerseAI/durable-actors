from __future__ import annotations

import inspect
from collections.abc import Callable, Generator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any


@dataclass
class Scope:
    actor: object
    register: Callable[[Callable[[], Any]], None]
    active: bool = True


invocation: ContextVar[Scope] = ContextVar("actor_background_tasks")


def wait_until(actor: object, task: Callable[[], Any]) -> None:
    scope = invocation.get(None)
    if scope is None or scope.actor is not actor or not scope.active:
        raise RuntimeError("background tasks require an active actor invocation")
    if not callable(task) or inspect.iscoroutinefunction(task) or inspect.isgeneratorfunction(task):
        raise TypeError("wait_until requires a deferred callback")
    scope.register(task)


class BackgroundTasks:
    def __init__(self) -> None:
        self.sequence = 0
        self.pending: dict[int, Callable[[], Any]] = {}

    @contextmanager
    def scope(self, actor: object, interleaved: bool) -> Generator[list[int], None, None]:
        tasks: list[int] = []

        def register(task: Callable[[], Any]) -> None:
            if interleaved:
                raise RuntimeError("background tasks require a serialized actor")
            if len(self.pending) >= 64:
                raise RuntimeError("actor background task limit reached")
            self.sequence += 1
            self.pending[self.sequence] = task
            tasks.append(self.sequence)

        scope = Scope(actor, register)
        token = invocation.set(scope)
        try:
            yield tasks
        except BaseException:
            self.discard(tasks)
            raise
        finally:
            scope.active = False
            invocation.reset(token)

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
