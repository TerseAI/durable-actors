import time
from pathlib import Path
from typing import Any

from durable_actors import Actor, persisted


class ResponsiveSession(Actor[None, None, dict[str, str]]):
    generation: int = persisted(0)
    state: str = persisted("idle")
    heartbeats: int = persisted(0)

    def start(self, gate: str, fail: bool, pair: bool = False) -> int:
        self.generation += 1
        self.state = "provisioning"
        work = {"gate": gate, "generation": self.generation, "fail": fail}
        self.run_task(type(self)._provision, work, "complete")
        if pair:
            self.run_task(type(self)._provision, {**work, "gate": gate + ".sibling"}, "complete")
        work["generation"] = -1
        return self.generation

    @staticmethod
    def _provision(work: dict[str, Any]) -> str:
        Path(work["gate"] + ".entered").touch()
        deadline = time.monotonic() + 10
        while not Path(work["gate"]).exists():
            if time.monotonic() >= deadline:
                raise TimeoutError("gate was not released")
            time.sleep(0.01)
        if work["fail"]:
            raise ValueError("provisioning failed")
        work["generation"] = -2
        return "ready"

    def complete(self, outcome: dict[str, str | int | bool | dict[str, str | int | bool]]) -> None:
        if outcome["input"]["generation"] == self.generation:
            self.state = outcome["value"] if outcome["ok"] else "failed"
        self.broadcast({"state": self.state})

    def cancel(self) -> None:
        self.generation += 1
        self.state = "canceled"

    def heartbeat(self) -> int:
        self.heartbeats += 1
        return self.heartbeats

    def finish_terminal(self) -> None:
        self.state = "terminal_done"

    def fail(self) -> None:
        self.state = "corrupt"
        raise ValueError("rollback")

    def read(self) -> dict[str, str | int]:
        return {"state": self.state, "heartbeats": self.heartbeats}
