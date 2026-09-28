import asyncio
import os
import socket
import tempfile
from pathlib import Path

import httpx

from little_actors import Client


async def main() -> None:
    with tempfile.TemporaryDirectory() as directory, socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
        listener.close()
        Path(directory, "actors.py").write_text("""from typing import Annotated
from little_actors import Actor, Persisted
class Counter(Actor):
    count: Annotated[int, Persisted()] = 0
    async def increment(self) -> int:
        self.count += 1
        return self.count
""")
        for expected in (1, 2):
            runtime = await asyncio.create_subprocess_exec(
                "durable-actors",
                "dev",
                "--project",
                directory,
                "--entrypoint",
                "actors.py",
                "--port",
                str(port),
                env={**os.environ, "DURABLE_ACTORS_PYTHON": "python3"},
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                async with asyncio.timeout(30), Client(f"http://127.0.0.1:{port}") as client:
                    while True:
                        if runtime.returncode is not None:
                            raise RuntimeError(await runtime.communicate())
                        try:
                            contract = await client.get_contract()
                            assert contract["actors"][0]["actorName"] == "Counter"
                            break
                        except (httpx.TransportError, httpx.HTTPStatusError):
                            await asyncio.sleep(0.1)
                    assert await client.invoke("Counter", "one", "increment", []) == expected
            finally:
                if runtime.returncode is None:
                    runtime.terminate()
                async with asyncio.timeout(10):
                    await runtime.communicate()
    print("Python actor image: invocation and durable restart passed")


asyncio.run(main())
