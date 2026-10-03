import json
import os
import shutil
import time
from pathlib import Path
from threading import Timer

import pytest
from test_integration import actor_server, free_port
from websockets.sync.client import connect


@pytest.mark.skipif(
    not os.environ.get("DURABLE_ACTORS_TEST_RUNTIME"), reason="requires built Rust runtime"
)
@pytest.mark.parametrize(
    "language",
    [
        "python",
        pytest.param(
            "typescript",
            marks=pytest.mark.skipif(
                os.environ.get("DURABLE_ACTORS_TEST_TYPESCRIPT") != "1",
                reason="requires built TypeScript SDK and Bun",
            ),
        ),
    ],
)
def test_background_response_idle_lifetime_and_shutdown_recovery(tmp_path, monkeypatch, language):
    root = Path(__file__).resolve().parents[2]
    entrypoint = "actors.py" if language == "python" else "actors.ts"
    source = (
        Path(__file__).parent / "fixtures/background.py"
        if language == "python"
        else root / "sdk/tests/fixtures/background-actor.ts"
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
    monkeypatch.setenv("DURABLE_ACTORS_HOST_IDLE_TIMEOUT_MS", "500")
    first, second = tmp_path / "first", tmp_path / "second"
    timer = None
    try:
        with actor_server(tmp_path, entrypoint, free_port()) as (client, _):
            with connect(
                client.prepare_websocket("BackgroundProbe", "one", None).websocket_url
            ) as connection:
                instance = client.invoke("BackgroundProbe", "one", "start", [str(first)])
                assert not first.exists()
                time.sleep(0.8)
                first.touch()
                assert json.loads(connection.recv(timeout=5)) == {"count": 2}
                assert client.invoke("BackgroundProbe", "one", "read", []) == {
                    "count": 2,
                    "instance": instance,
                }
            client.invoke("BackgroundProbe", "one", "siblings", [])
            assert client.invoke("BackgroundProbe", "one", "read", [])["count"] == 3
            client.invoke("BackgroundProbe", "one", "start", [str(second)])
            timer = Timer(0.5, second.touch)
            timer.start()
        assert second.exists(), "shutdown returned before the pending callback was released"
        with actor_server(tmp_path, entrypoint, free_port()) as (client, _):
            recovered = client.invoke("BackgroundProbe", "one", "read", [])
            assert recovered["count"] == 5
            assert recovered["instance"] != instance
    finally:
        first.touch()
        second.touch()
        if timer:
            timer.cancel()
