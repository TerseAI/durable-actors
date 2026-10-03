import time
from pathlib import Path
from uuid import uuid4

from durable_actors import Actor, ephemeral, persisted


class BackgroundProbe(Actor[None, None, dict[str, int]]):
    count: int = persisted(0)
    instance: str = ephemeral(default_factory=lambda: str(uuid4()))

    def start(self, gate: str) -> str:
        self.count += 1

        def work() -> None:
            deadline = time.monotonic() + 10
            while not Path(gate).exists():
                if time.monotonic() >= deadline:
                    raise TimeoutError("test did not release background task")
                time.sleep(0.01)
            self.count += 1
            self.broadcast({"count": self.count})

        self.wait_until(work)
        return self.instance

    def siblings(self) -> None:
        def fail() -> None:
            self.count = 99
            raise ValueError("background failed")

        self.wait_until(fail)
        self.wait_until(lambda: setattr(self, "count", self.count + 1))

    def read(self) -> dict[str, str | int]:
        return {"count": self.count, "instance": self.instance}
