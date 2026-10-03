import json
import os
import time
from pathlib import Path

import pytest
from test_integration import actor_server, free_port

from durable_actors import ActorInvocationError


@pytest.mark.skipif(
    not os.environ.get("DURABLE_ACTORS_TEST_RUNTIME"), reason="requires built Rust runtime"
)
@pytest.mark.parametrize("language", ["python", "typescript"])
def test_alarm_deadlines_retry_and_wake_after_restart(tmp_path, monkeypatch, language):
    sdk = Path(__file__).parents[2] / "sdk"
    monkeypatch.setenv("DURABLE_ACTORS_SDK_HOST", str(sdk / "dist/host.js"))
    fired = tmp_path / "fired"
    crashed = tmp_path / "crashed"
    if language == "typescript":
        entrypoint = "actors.ts"
        source = """import { Actor, Persisted } from SDK;
import { existsSync, writeFileSync } from "node:fs";
export class Timer extends Actor {
    @Persisted count = 0;
    @Persisted mode = "normal";
    async schedule(deadline: number, mode: string): Promise<void> { this.mode = mode; this.setAlarm(deadline); }
    async rollback(): Promise<void> { this.setAlarm(0); throw new Error("rollback"); }
    async cancel(): Promise<void> { this.deleteAlarm(); }
    async current(): Promise<number | null> { return this.getAlarm(); }
    async read(): Promise<number> { return this.count; }
    async onAlarm(): Promise<void> {
        if (this.mode === "crash" && !existsSync(CRASHED)) { writeFileSync(CRASHED, "crashed"); process.exit(17); }
        if (this.mode === "retry" && !existsSync(CRASHED)) { writeFileSync(CRASHED, "failed"); throw new Error("retry me"); }
        this.count++;
        writeFileSync(FIRED, String(this.count));
        if (this.mode === "rearm") { this.mode = "normal"; this.setAlarm(Date.now() + 250); }
    }
}
""".replace("SDK", json.dumps(str(sdk / "dist/index.js")))
        (tmp_path / "tsconfig.json").write_text(
            json.dumps(
                {
                    "compilerOptions": {
                        "target": "ES2022",
                        "module": "NodeNext",
                        "moduleResolution": "NodeNext",
                        "strict": True,
                        "skipLibCheck": True,
                        "types": ["node"],
                        "typeRoots": [str(sdk / "node_modules/@types")],
                    },
                    "include": ["actors.ts"],
                }
            )
        )
    else:
        entrypoint = "actors.py"
        source = """from durable_actors import Actor, persisted
from pathlib import Path
import os, time
class Timer(Actor):
    count: int = persisted(0)
    mode: str = persisted("normal")
    def schedule(self, deadline: int, mode: str) -> None:
        self.mode = mode
        self.set_alarm(deadline)
    def rollback(self) -> None:
        self.set_alarm(0)
        raise ValueError("rollback")
    def cancel(self) -> None:
        self.delete_alarm()
    def current(self) -> int | None:
        return self.get_alarm()
    def read(self) -> int:
        return self.count
    def on_alarm(self) -> None:
        if self.mode == "crash" and not Path(CRASHED).exists():
            Path(CRASHED).write_text("crashed")
            os._exit(17)
        if self.mode == "retry" and not Path(CRASHED).exists():
            Path(CRASHED).write_text("failed")
            raise ValueError("retry me")
        self.count += 1
        Path(FIRED).write_text(str(self.count))
        if self.mode == "rearm":
            self.mode = "normal"
            self.set_alarm(time.time_ns() // 1_000_000 + 250)
"""
    (tmp_path / entrypoint).write_text(
        source.replace("FIRED", json.dumps(str(fired))).replace("CRASHED", json.dumps(str(crashed)))
    )
    port = free_port()

    def wait_fired(expected):
        end = time.monotonic() + 20
        while time.monotonic() < end:
            if fired.exists() and fired.read_text() == str(expected):
                return
            time.sleep(0.1)
        pytest.fail((tmp_path / "runtime.log").read_text())

    with actor_server(tmp_path, entrypoint, port, sdk_host=sdk / "dist/host.js") as (client, _):

        def call(method, *args):
            return client.invoke("Timer", "one", method, list(args))

        call("schedule", 0, "normal")
        wait_fired(1)
        assert call("read") == 1
        with pytest.raises(ActorInvocationError):
            call("__alarm", "forged")
        with pytest.raises(ActorInvocationError):
            call("rollback")
        assert call("current") is None
        later = time.time_ns() // 1_000_000 + 800
        call("schedule", later, "normal")
        call("schedule", later + 400, "normal")
        assert call("current") == later + 400
        call("cancel")
        time.sleep(1.5)
        assert call("read") == 1
        call("schedule", 0, "rearm")
        wait_fired(3)
        call("schedule", 0, "retry")
        wait_fired(4)
        crashed.unlink()
        call("schedule", 0, "crash")
        wait_fired(5)
        assert call("read") == 5
        call("schedule", time.time_ns() // 1_000_000 + 3_000, "normal")
    with actor_server(tmp_path, entrypoint, port, sdk_host=sdk / "dist/host.js") as (client, _):
        wait_fired(6)
        assert client.invoke("Timer", "one", "read", []) == 6
