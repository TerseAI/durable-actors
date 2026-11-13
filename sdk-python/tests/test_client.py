import errno
import json
import os
import subprocess
import sys

import httpx
import pytest

from durable_actors import AsyncClient
from durable_actors.client import ActorInvocationError, Client


def test_gateway_calls_retry_only_explicit_non_execution():
    calls = []

    def handle(request):
        calls.append(request)
        outcome = (
            {"type": "not_executed", "reason": "host_unavailable"}
            if len(calls) == 2
            else {"type": "completed", "result": 3}
        )
        return httpx.Response(200, json={"outcome": outcome})

    with httpx.Client(transport=httpx.MockTransport(handle)) as http:
        with Client("http://control.test", project_id="test", api_key="key", http=http) as client:
            assert client.invoke("Counter", "one", "increment", []) == 3
            assert client.invoke("Counter", "one", "increment", []) == 3
    assert [request.url.host for request in calls] == ["control.test"] * 3
    assert all(request.headers["authorization"] == "Bearer key" for request in calls)
    assert calls[1].content == calls[2].content


def test_client_sends_the_invoke_api_key_when_no_secret_is_set(monkeypatch):
    monkeypatch.delenv("DURABLE_ACTORS_SECRET", raising=False)
    monkeypatch.delenv("DURABLE_ACTORS_API_KEY", raising=False)
    monkeypatch.setenv("DURABLE_ACTORS_INVOKE_API_KEY", "invoke-key")
    with Client("http://control.test", project_id="test") as client:
        assert client.headers == {"authorization": "Bearer invoke-key"}


def refused():
    error = httpx.ConnectError("[Errno 61] Connection refused")
    error.__cause__ = ConnectionRefusedError(errno.ECONNREFUSED, "Connection refused")
    return error


NEVER_SENT = pytest.mark.parametrize(
    "failure",
    [
        refused,
        lambda: httpx.ConnectError("[Errno 8] nodename nor servname provided"),
        lambda: httpx.ConnectTimeout("timed out"),
    ],
    ids=["refused", "dns", "connect_timeout"],
)
MAYBE_SENT = pytest.mark.parametrize(
    "failure",
    [
        lambda: httpx.ReadTimeout("timed out"),
        lambda: httpx.ReadError("response lost"),
        lambda: httpx.RemoteProtocolError("server disconnected"),
    ],
    ids=["read_timeout", "read_error", "disconnected"],
)


def invoke_through(failing_host, failure):
    calls = []

    def handle(request):
        calls.append(request.url.host)
        if request.url.host == failing_host:
            raise failure()
        outcome = {"type": "completed", "result": len(calls)}
        return httpx.Response(200, json={"outcome": outcome})

    with httpx.Client(transport=httpx.MockTransport(handle)) as http:
        with Client("http://control.test", project_id="test", http=http) as client:
            try:
                return client.invoke("Counter", "one", "increment", []), calls
            except ActorInvocationError as error:
                return error, calls


@NEVER_SENT
def test_control_plane_request_that_was_never_sent_is_unavailable(failure):
    error, calls = invoke_through("control.test", failure)
    assert isinstance(error, ActorInvocationError)
    assert error.code == "unavailable"
    assert str(error) == f"could not connect to http://control.test: {failure()}"
    assert calls == ["control.test"]


@MAYBE_SENT
def test_control_plane_request_that_may_have_been_sent_is_not_replayed(failure):
    error, calls = invoke_through("control.test", failure)
    assert isinstance(error, ActorInvocationError)
    assert error.code == "outcome_unknown"
    assert calls == ["control.test"]


def test_default_client_shares_its_pool_and_closes_it_at_process_exit(tmp_path):
    closed = tmp_path / "closed"
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sys
import httpx
from durable_actors.client import default_client

class TrackedHttpClient(httpx.Client):
    def close(self):
        super().close()
        assert self.is_closed
        Path(sys.argv[1]).write_text("closed")

httpx.Client = TrackedHttpClient
with ThreadPoolExecutor(max_workers=8) as pool:
    clients = list(pool.map(lambda _: default_client(), range(32)))
assert all(client is clients[0] for client in clients)
""",
            str(closed),
        ],
        env={**os.environ, "DURABLE_ACTORS_CONTROL_PLANE_URL": "http://127.0.0.1:7100"},
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert closed.read_text() == "closed"


def test_warm_sends_the_duration_with_the_caller_credential():
    calls = []

    def handle(request):
        calls.append(request)
        return httpx.Response(200, json={"status": "OK"})

    with httpx.Client(transport=httpx.MockTransport(handle)) as http:
        with Client(
            "http://control.test",
            project_id="test",
            api_key="key",
            home_region="north-america-east",
            http=http,
        ) as client:
            assert client.warm("Counter", "one", 30_000) == {"status": "OK"}
    assert len(calls) == 1
    assert calls[0].url == "http://control.test/v1/projects/test/actors/Counter/one/warm-host"
    assert calls[0].headers["authorization"] == "Bearer key"
    body = json.loads(calls[0].content)
    assert body == {
        "requestId": body["requestId"],
        "warmMs": 30_000,
        "homeRegion": "north-america-east",
    }


def test_warm_sends_wait_for_completion_and_timeout(monkeypatch):
    monkeypatch.setenv("DURABLE_ACTORS_HOME_REGION", "north-america-east")
    bodies = []

    def handle(request):
        bodies.append(json.loads(request.content))
        return httpx.Response(202, json={"status": "accepted"})

    with httpx.Client(transport=httpx.MockTransport(handle)) as http:
        with Client("http://control.test", project_id="test", api_key="key", http=http) as client:
            assert client.warm("Counter", "one", 30_000, wait_for_completion=False) == {
                "status": "accepted"
            }
            assert client.warm(
                "Counter", "one", 30_000, wait_for_completion=False, timeout_ms=2000
            ) == {"status": "accepted"}
    assert [
        {key: value for key, value in body.items() if key != "requestId"} for body in bodies
    ] == [
        {"warmMs": 30_000, "waitForCompletion": False, "homeRegion": "north-america-east"},
        {
            "warmMs": 30_000,
            "waitForCompletion": False,
            "timeoutMs": 2000,
            "homeRegion": "north-america-east",
        },
    ]


async def test_async_warm_sends_the_duration_and_returns_the_status():
    calls = []

    async def handle(request):
        calls.append(request)
        return httpx.Response(200, json={"status": "OK"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        async with AsyncClient(
            "http://control.test",
            project_id="test",
            api_key="key",
            home_region="north-america-east",
            http=http,
        ) as client:
            assert await client.warm("Counter", "one", 30_000) == {"status": "OK"}
    assert len(calls) == 1
    assert calls[0].url == "http://control.test/v1/projects/test/actors/Counter/one/warm-host"
    assert calls[0].headers["authorization"] == "Bearer key"
    body = json.loads(calls[0].content)
    assert body == {
        "requestId": body["requestId"],
        "warmMs": 30_000,
        "homeRegion": "north-america-east",
    }


def test_warm_rejects_a_duration_outside_the_range_before_sending():
    calls = []

    def handle(request):
        calls.append(request)
        return httpx.Response(200, json={"status": "OK"})

    with httpx.Client(transport=httpx.MockTransport(handle)) as http:
        with Client("http://control.test", project_id="test", api_key="key", http=http) as client:
            for warm_ms in [-5, 0, 1.5, True, 86_400_001]:
                with pytest.raises(ValueError):
                    client.warm("Counter", "one", warm_ms)
    assert calls == []
