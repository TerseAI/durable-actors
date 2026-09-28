import httpx
import pytest

from little_actors.client import ActorInvocationError, Client


def test_caches_route_and_retries_only_explicit_non_execution():
    calls = []
    target = {
        "route": "http://host.test",
        "token": "ticket",
        "ownerEpoch": 1,
        "expiresAtMs": 9999999999999,
    }

    def handle(request):
        calls.append(request.url.host)
        if request.url.host == "host.test":
            return httpx.Response(200, json={"type": "not_executed", "reason": "stale_owner"})
        return httpx.Response(
            200, json={"target": target, "outcome": {"type": "completed", "result": 3}}
        )

    with httpx.Client(transport=httpx.MockTransport(handle)) as http:
        with Client("http://control.test", project_id="test", http=http) as client:
            assert client.invoke("Counter", "one", "increment", []) == 3
            assert client.invoke("Counter", "one", "increment", []) == 3
    assert calls == ["control.test", "host.test", "control.test"]


def test_lost_response_is_not_replayed():
    calls = []

    def handle(request):
        calls.append(request)
        raise httpx.ReadError("response lost")

    with httpx.Client(transport=httpx.MockTransport(handle)) as http:
        with Client("http://control.test", project_id="test", http=http) as client:
            with pytest.raises(ActorInvocationError) as failure:
                client.invoke("Counter", "one", "increment", [])
    assert failure.value.code == "outcome_unknown"
    assert len(calls) == 1
