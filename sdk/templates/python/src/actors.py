from typing import Annotated

from little_actors import Actor, Persisted


class Counter(Actor):
    count: Annotated[int, Persisted()] = 0

    async def increment(self, amount: int = 1) -> int:
        self.count += amount
        return self.count
