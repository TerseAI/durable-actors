from asyncio import Lock
from typing import assert_type

from little_actors import Actor, emitted, ephemeral


class TypedActor(Actor):
    count: int = 0
    version: int = emitted(0)
    messages: list[str] = emitted(default_factory=list)
    busy: bool = ephemeral(False)
    lock: Lock = ephemeral(default_factory=Lock)

    async def append(self, message: str) -> int:
        async with self.lock:
            self.messages.append(message)
            self.count += 1
        assert_type(self.count, int)
        assert_type(self.version, int)
        assert_type(self.messages, list[str])
        assert_type(self.busy, bool)
        assert_type(self.lock, Lock)
        return self.count
