import httpx
import pytest

pytest.importorskip("starlette")
pytest.importorskip("uvicorn")

from durable_actors.client import Client
from durable_actors.observer import observer_app


async def test_logs_proxy_streams_despite_history_failure_and_keeps_credentials_server_side(
    tmp_path, monkeypatch
):
    original_client = httpx.AsyncClient
    requests = []

    def upstream(request):
        requests.append(request)
        assert request.headers["authorization"] == "Bearer private-key"
        if request.url.path.endswith("/logs/events"):
            return httpx.Response(
                200,
                content=b'event: logs\ndata: {"records":[]}\n\n',
                headers={"content-type": "text/event-stream"},
            )
        return httpx.Response(503, text="private-key")

    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda **options: original_client(transport=httpx.MockTransport(upstream), **options),
    )
    with Client("https://control.example", project_id="project", api_key="private-key") as client:
        app = observer_app(client, tmp_path, "127.0.0.1:4321")
        async with original_client(
            transport=httpx.ASGITransport(app=app), base_url="http://127.0.0.1:4321"
        ) as http:
            filters = {"actorName": "Counter", "actorId": "a/b", "level": "WARN"}
            response = await http.get(
                "/api/observe/logs", params={**filters, "cursor": "next+page"}
            )
            assert response.status_code == 503
            assert "private-key" not in response.text
            stream = await http.get("/api/observe/logs/events", params=filters)
            assert stream.status_code == 200
            assert stream.headers["x-accel-buffering"] == "no"
            assert stream.text.startswith("event: logs")
            assert str(requests[0].url) == (
                "https://control.example/v1/projects/project/observe/logs"
                "?actorName=Counter&actorId=a%2Fb&level=WARN&cursor=next%2Bpage"
            )
            assert dict(requests[1].url.params) == filters
            assert (await http.head("/api/observe/logs/events")).status_code == 200
            assert len(requests) == 2
            assert (await http.post("/api/observe/logs")).status_code == 405
            assert (
                await http.get(
                    "/api/observe/logs/events", headers={"origin": "https://evil.example"}
                )
            ).status_code == 403
