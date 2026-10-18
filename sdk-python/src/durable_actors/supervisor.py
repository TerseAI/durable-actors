"""Keep executor communication alive while replaceable processes run actor code."""

from __future__ import annotations

import asyncio
import os
import socket
import sys
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass, field

from pydantic import TypeAdapter

from .contract import Document
from .executor_wire import Channel
from .runtime import failed

Environment = dict[str, str]
# A graceful eviction gives on_sleep this long before the worker is stopped anyway.
EVICTION_GRACE_SECONDS = 30.0
environment_adapter: TypeAdapter[Environment] = TypeAdapter(Environment)


@dataclass
class Pending:
    exchanges: set[str] = field(default_factory=set[str])
    reply: Document | None = None


class Supervisor(Channel):
    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        create_worker: Callable[[str, Environment], Awaitable[Worker]],
    ) -> None:
        super().__init__(reader, writer)
        self.create_worker = create_worker
        self.worker: Worker | None = None
        self.pump: asyncio.Task[None] | None = None
        self.pending: dict[int, Pending] = {}
        self.attached: Document = {}
        self.entrypoint = ""
        self.environment: Environment = {}
        self.actor_names: list[str] = []
        self.sequence = 0
        self.offset = 0
        # Message ID of a graceful eviction the worker is running on_sleep for.
        self.evicting: int | None = None
        self.eviction_done = False
        self.deferred: list[Document] = []
        self.eviction_tasks: set[asyncio.Task[None]] = set()

    async def run(self, entrypoint: str | None, generic: bool) -> None:
        from .host import attached_actor

        try:
            if generic:
                await self.send({"type": "warm", "protocol": 28})
                load = await self.read()
                if load.get("type") != "load":
                    raise ValueError("expected actor code assignment")
                entrypoint = load.get("entrypoint")
                self.environment = environment_adapter.validate_python(
                    load.get("environment"), strict=True
                )
            if not entrypoint or not os.path.isabs(entrypoint):
                raise ValueError("an absolute actor entrypoint is required")
            self.entrypoint = entrypoint
            self.worker = await self.create_worker(entrypoint, self.environment)
            self.actor_names = self.worker.actor_names
            await self.send({"type": "attach", "protocol": 28, "actor_names": self.actor_names})
            self.attached = await self.read()
            attached_actor(self.attached, self.actor_names)
            await self.start_pump()
            while True:
                await self.accept(await self.read())
        finally:
            for task in tuple(self.eviction_tasks):
                task.cancel()
            await self.stop_worker()
            self.writer.close()
            with suppress(ConnectionError):
                await self.writer.wait_closed()

    async def accept(self, message: Document) -> None:
        if message["type"] != "command":
            await self.acknowledge(message)
            return
        from .host import validate_command

        validate_command(message)
        if self.evicting is not None:
            # Commands that arrive while on_sleep runs wait for a fresh worker.
            self.deferred.append(message)
            return
        await self.dispatch(message)

    async def dispatch(self, message: Document) -> None:
        command, identifier = message["command"], message["message_id"]
        if command["type"] == "evict":
            if self.worker is not None and command.get("graceful", True) and not self.pending:
                if identifier in self.pending:
                    raise ValueError("executor message ID is duplicated")
                # An idle graceful eviction goes to the worker so it runs on_sleep before it
                # stops. Busy evictions stop the worker at once: a blocked sync handler cannot
                # be cancelled, and on_sleep must not run beside it.
                self.pending[identifier] = Pending()
                self.evicting = identifier
                self.eviction_done = False
                await self.worker.send(message)
                self.spawn(self.expire_eviction(identifier))
            else:
                await self.stop_worker()
                await self.fail_pending("actor_evicted", "actor was evicted by the Rust host")
                await self.reply(identifier, {"type": "evicted"})
        else:
            if len(self.pending) >= 512 or identifier in self.pending:
                raise ValueError("executor command queue is full or message ID is duplicated")
            if self.worker is None:
                self.worker = await self.create_worker(self.entrypoint, self.environment)
                if self.worker.actor_names != self.actor_names:
                    raise ValueError("worker actor definitions changed")
                self.offset = self.sequence
                await self.start_pump()
            self.pending[identifier] = Pending()
            await self.worker.send(message)

    async def acknowledge(self, message: Document) -> None:
        kind, identifier = message["type"], message["message_id"]
        pending = self.pending.get(identifier)
        if pending is None or kind not in pending.exchanges:
            raise ValueError("unexpected executor acknowledgment")
        pending.exchanges.remove(kind)
        if pending.reply is not None:
            await self.complete(identifier)
        elif self.worker is not None:
            await self.worker.send(message)

    async def start_pump(self) -> None:
        assert self.worker is not None
        await self.worker.send(self.attached)
        self.pump = asyncio.create_task(self.forward(self.worker))

    async def forward(self, worker: Worker) -> None:
        try:
            while True:
                message = await worker.read()
                if message["type"] == "residency":
                    await self.send(message)
                    continue
                identifier = message["message_id"]
                pending = self.pending[identifier]
                kind = message["type"]
                if kind == "reply":
                    reply = message["reply"]
                    if "sequence" in reply:
                        reply["sequence"] += self.offset
                        self.sequence = max(self.sequence, reply["sequence"])
                    pending.reply = reply
                    await self.complete(identifier)
                else:
                    if kind == "socket_effects":
                        pending.exchanges.add("socket_effects_published")
                    elif kind == "database_call":
                        pending.exchanges.add("database_result")
                    elif kind == "get_connections":
                        pending.exchanges.add("socket_connections")
                    elif kind != "ready_for_invocation":
                        raise ValueError("unexpected worker message")
                    await self.send(message)
        except (EOFError, ConnectionError, ValueError, KeyError) as error:
            await worker.close()
            if self.worker is worker:
                self.worker = None
                await self.fail_pending("actor_worker_failed", str(error))

    async def fail_pending(self, code: str, message: str) -> None:
        for identifier in list(self.pending):
            self.pending[identifier].reply = failed(code, message)
            await self.complete(identifier)
        await self.send({"type": "residency", "resident": False})

    async def complete(self, identifier: int) -> None:
        pending = self.pending[identifier]
        if pending.reply is not None and not pending.exchanges:
            del self.pending[identifier]
            if identifier == self.evicting:
                # This can run inside the pump, which finish_eviction cancels.
                self.spawn(self.finish_eviction(identifier))
            else:
                await self.reply(identifier, pending.reply)

    def spawn(self, coroutine: Awaitable[None]) -> None:
        task = asyncio.ensure_future(coroutine)
        self.eviction_tasks.add(task)
        task.add_done_callback(self.eviction_tasks.discard)

    async def expire_eviction(self, identifier: int) -> None:
        await asyncio.sleep(EVICTION_GRACE_SECONDS)
        await self.finish_eviction(identifier)

    async def finish_eviction(self, identifier: int) -> None:
        if self.evicting != identifier or self.eviction_done:
            return
        self.eviction_done = True
        self.pending.pop(identifier, None)
        await self.stop_worker()
        await self.fail_pending("actor_evicted", "actor was evicted by the Rust host")
        await self.reply(identifier, {"type": "evicted"})
        self.evicting = None
        while self.deferred and self.evicting is None:
            await self.dispatch(self.deferred.pop(0))

    async def reply(self, identifier: int, value: Document) -> None:
        await self.send({"type": "reply", "message_id": identifier, "reply": value})

    async def stop_worker(self) -> None:
        if self.pump is not None:
            self.pump.cancel()
            await asyncio.gather(self.pump, return_exceptions=True)
            self.pump = None
        if self.worker is not None:
            await self.worker.close()
            self.worker = None


class Worker(Channel):
    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        process: asyncio.subprocess.Process,
    ) -> None:
        super().__init__(reader, writer)
        self.process = process
        self.actor_names: list[str] = []

    @classmethod
    async def start(cls, entrypoint: str, environment: Environment) -> Worker:
        parent, child = socket.socketpair()
        try:
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                "-m",
                "durable_actors.host",
                "--worker",
                str(child.fileno()),
                env={**os.environ, **environment, "DURABLE_ACTORS_ENTRYPOINT": entrypoint},
                pass_fds=(child.fileno(),),
            )
        except BaseException:
            parent.close()
            raise
        finally:
            child.close()
        worker: Worker | None = None
        try:
            reader, writer = await asyncio.open_connection(sock=parent, limit=sys.maxsize)
            worker = cls(reader, writer, process)
            async with asyncio.timeout(60):
                attach = await worker.read()
            if attach.get("type") != "attach" or attach.get("protocol") != 28:
                raise ValueError("worker did not attach")
            worker.actor_names = attach["actor_names"]
            return worker
        except BaseException:
            if worker is not None:
                await worker.close()
            else:
                parent.close()
                with suppress(ProcessLookupError):
                    process.kill()
                await process.wait()
            raise

    async def close(self) -> None:
        if self.process.returncode is None:
            with suppress(ProcessLookupError):
                self.process.kill()
        await self.process.wait()
        self.writer.close()
        with suppress(ConnectionError):
            await self.writer.wait_closed()
