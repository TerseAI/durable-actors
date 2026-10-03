import json
import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Timer

import pytest
from test_integration import actor_server, free_port
from websockets.sync.client import connect


@pytest.mark.skipif(
    not os.environ.get("DURABLE_ACTORS_TEST_RUNTIME"), reason="requires built Rust runtime"
)
@pytest.mark.parametrize("language", ["python", "typescript"])
def test_external_work_preserves_responsive_serial_execution_and_restart(
    tmp_path, monkeypatch, language
):
    if language == "typescript" and not os.environ.get("DURABLE_ACTORS_TEST_TYPESCRIPT"):
        pytest.skip("requires built TypeScript SDK and Bun")
    monkeypatch.setenv("DURABLE_ACTORS_HOST_IDLE_TIMEOUT_MS", "100")
    root = Path(__file__).resolve().parents[2]
    entrypoint = "actors.py" if language == "python" else "actors.ts"
    source = (
        Path(__file__).parent / "fixtures/responsive.py"
        if language == "python"
        else root / "sdk/tests/fixtures/responsive-actor.ts"
    )
    shutil.copyfile(source, tmp_path / entrypoint)
    if language == "typescript":
        (tmp_path / "node_modules").mkdir()
        (tmp_path / "node_modules/durable-actors").symlink_to(root / "sdk")
        (tmp_path / "package.json").write_text('{"type":"module"}')
        (tmp_path / "tsconfig.json").write_text(
            json.dumps(
                {
                    "compilerOptions": {
                        "target": "ES2022",
                        "module": "NodeNext",
                        "strict": True,
                        "skipLibCheck": True,
                        "typeRoots": [str(root / "sdk/node_modules/@types")],
                    }
                }
            )
        )
    gates = [tmp_path / "canceled", tmp_path / "failed", tmp_path / "success"]
    drain = tmp_path / "drain"
    timer = None
    with ThreadPoolExecutor() as pool:
        try:
            with actor_server(tmp_path, entrypoint, free_port()) as (client, _):

                def call(name, args=None):
                    return client.invoke("ResponsiveSession", "one", name, args or [])

                with connect(
                    client.prepare_websocket("ResponsiveSession", "one", None).websocket_url
                ) as connection:
                    for index, gate in enumerate(gates):
                        call("start", [str(gate), index == 1, index == 0])
                        deadline = time.monotonic() + 5
                        while not Path(str(gate) + ".entered").exists():
                            assert time.monotonic() < deadline, "external task did not start"
                            time.sleep(0.01)
                        if index == 0:
                            time.sleep(0.2)
                        assert pool.submit(call, "heartbeat").result(timeout=2) == index + 1
                        if index == 0:
                            pool.submit(call, "cancel").result(timeout=2)
                            pool.submit(call, "finish_terminal").result(timeout=2)
                            with pytest.raises(Exception, match="rollback"):
                                pool.submit(call, "fail").result(timeout=2)
                            assert call("read")["state"] == "terminal_done"
                        gate.touch()
                        assert json.loads(connection.recv(timeout=5)) == {
                            "state": ["terminal_done", "failed", "ready"][index]
                        }
                        if index == 0:
                            sibling = Path(str(gate) + ".sibling")
                            deadline = time.monotonic() + 5
                            while not Path(str(sibling) + ".entered").exists():
                                assert time.monotonic() < deadline, (
                                    "committed sibling task was lost after rollback"
                                )
                                time.sleep(0.01)
                            sibling.touch()
                            assert json.loads(connection.recv(timeout=5)) == {
                                "state": "terminal_done"
                            }
                call("start", [str(drain), False])
                timer = Timer(0.3, drain.touch)
                timer.start()
            assert drain.exists(), "shutdown returned before external work completed"
            with actor_server(tmp_path, entrypoint, free_port()) as (client, _):
                assert client.invoke("ResponsiveSession", "one", "read", []) == {
                    "state": "ready",
                    "heartbeats": 3,
                }
        finally:
            Path(str(gates[0]) + ".sibling").touch()
            drain.touch()
            if timer:
                timer.cancel()
            for gate in gates:
                gate.touch()
