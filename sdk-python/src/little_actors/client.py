from __future__ import annotations

import json
import os
import re
import time
import uuid
from types import TracebackType
from typing import Any, Protocol, cast
from urllib.parse import urlsplit

import httpx
from pydantic import BaseModel, ConfigDict

from .guards import is_document


class ActorInvocationError(Exception):
    def __init__(self, code: str, request_id: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.request_id = request_id


class ActorProtocolError(Exception):
    pass


class SocketGrant(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        alias_generator=lambda name: {
            "websocket_url": "websocketUrl",
            "home_region": "homeRegion",
            "connect_by_ms": "connectByMs",
            "authorized_until_ms": "authorizedUntilMs",
        }[name],
    )
    websocket_url: str
    home_region: str
    connect_by_ms: int
    authorized_until_ms: int


class RpcTransport(Protocol):
    async def invoke(self, actor_name: str, actor_id: str, method: str, args: list[Any]) -> Any: ...


class ActorTransport(RpcTransport, Protocol):
    async def prepare_websocket(
        self,
        actor_name: str,
        actor_id: str,
        metadata: Any,
        *,
        authorization_lifetime_ms: int = 900000,
    ) -> SocketGrant: ...


class Client:
    def __init__(
        self,
        control_plane_url: str | None = None,
        *,
        project_id: str | None = None,
        api_key: str | None = None,
        home_region: str | None = None,
        http: httpx.AsyncClient | None = None,
    ) -> None:
        self.origin = validate_origin(
            control_plane_url
            or os.environ.get("DURABLE_ACTORS_CONTROL_PLANE_URL", "http://127.0.0.1:7100")
        )
        local = urlsplit(self.origin).hostname in {"localhost", "127.0.0.1", "::1"}
        self.project_id = component(
            project_id or os.environ.get("DURABLE_ACTORS_PROJECT_ID") or ("local" if local else ""),
            64,
        )
        key = (
            api_key
            if api_key is not None
            else os.environ.get("DURABLE_ACTORS_SECRET", os.environ.get("DURABLE_ACTORS_API_KEY"))
        )
        if key is not None and not key.strip():
            raise ValueError("API key must not be empty")
        self.headers = {"authorization": f"Bearer {key.strip()}"} if key is not None else {}
        self.home_region = home_region or os.environ.get("DURABLE_ACTORS_HOME_REGION")
        self._http = (
            http if http is not None else httpx.AsyncClient(timeout=180, follow_redirects=False)
        )
        self._owns_http = http is None
        self._closed = False
        self._targets: dict[tuple[str, str], dict[str, Any]] = {}

    async def __aenter__(self) -> Client:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        self._closed = True
        if self._owns_http:
            await self._http.aclose()

    async def invoke(self, actor_name: str, actor_id: str, method: str, args: list[Any]) -> Any:
        if self._closed:
            raise RuntimeError("client is closed")
        path = self.actor_path(actor_name, actor_id)
        component(method, 255)
        json.dumps(args, allow_nan=False)
        request_id = str(uuid.uuid4())
        key = actor_name, actor_id
        for attempt in range(2):
            target = self._targets.get(key)
            if target is not None and target["expiresAtMs"] <= time.time() * 1000:
                target = None
                self._targets.pop(key, None)
            try:
                reply = await self._invoke_attempt(path, request_id, method, args, target, key)
            except httpx.TransportError as error:
                self._targets.pop(key, None)
                if target is not None and isinstance(error, httpx.ConnectError) and refused(error):
                    reply = {"type": "not_executed", "reason": "upstream_not_reached"}
                else:
                    raise ActorInvocationError(
                        "outcome_unknown",
                        request_id,
                        "invocation response was lost; execution may have occurred",
                    ) from error
            kind = reply.get("type")
            if kind == "completed" and "result" in reply:
                return reply["result"]
            if (
                kind == "failed"
                and isinstance(reply.get("code"), str)
                and isinstance(reply.get("message"), str)
            ):
                raise ActorInvocationError(reply["code"], request_id, reply["message"])
            if kind != "unauthenticated" and not (
                kind == "not_executed"
                and reply.get("reason")
                in {"stale_owner", "host_unavailable", "upstream_not_reached"}
            ):
                raise ActorProtocolError("invalid invocation outcome")
            self._targets.pop(key, None)
            if attempt:
                raise ActorInvocationError(
                    "unauthenticated" if kind == "unauthenticated" else "unavailable",
                    request_id,
                    "actor rejected invocation before execution",
                )
        raise AssertionError("unreachable")

    async def prepare_websocket(
        self,
        actor_name: str,
        actor_id: str,
        metadata: Any,
        *,
        authorization_lifetime_ms: int = 900000,
    ) -> SocketGrant:
        if not 1000 <= authorization_lifetime_ms <= 86400000:
            raise ValueError("authorization lifetime must be between one second and one day")
        if len(json.dumps(metadata, allow_nan=False).encode()) > 65536:
            raise ValueError("socket metadata exceeds 64 KiB")
        response = await self._http.post(
            self.origin + self.actor_path(actor_name, actor_id) + "/find-websocket",
            headers=self.headers,
            json={"metadata": metadata, "authorizationLifetimeMs": authorization_lifetime_ms},
            follow_redirects=False,
        )
        response.raise_for_status()
        grant = SocketGrant.model_validate(response.json(), strict=True)
        if urlsplit(grant.websocket_url).scheme not in {"ws", "wss"}:
            raise ActorProtocolError("invalid websocket URL")
        return grant

    async def get_contract(self) -> dict[str, Any]:
        response = await self._http.get(
            f"{self.origin}/v1/projects/{self.project_id}/deployment/contract",
            headers=self.headers,
            follow_redirects=False,
        )
        response.raise_for_status()
        value = document(response)
        if not re.fullmatch(
            r"sha256:[a-f0-9]{64}", value.get("contractHash", "")
        ) or not isinstance(value.get("contract"), dict):
            raise ActorProtocolError("invalid contract publication")
        return cast(dict[str, Any], value["contract"])

    def actor_path(self, actor_name: str, actor_id: str) -> str:
        return f"/v1/projects/{self.project_id}/actors/{component(actor_name, 255)}/{component(actor_id, 128)}"

    async def _invoke_attempt(
        self,
        path: str,
        request_id: str,
        method: str,
        args: list[Any],
        target: dict[str, Any] | None,
        key: tuple[str, str],
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"requestId": request_id, "method": method, "args": args}
        headers = self.headers
        origin = self.origin
        if target is not None:
            body["ownerEpoch"] = target["ownerEpoch"]
            origin = target["route"]
            headers = {"authorization": f"Bearer {target['token']}"}
        elif self.home_region:
            body["homeRegion"] = self.home_region
        response = await self._http.post(
            origin + path + "/invoke",
            headers={**headers, "x-request-id": request_id},
            json=body,
            follow_redirects=False,
        )
        if target is not None and response.status_code == 401:
            return {"type": "unauthenticated"}
        value = document(response)
        if not response.is_success:
            error = value.get("error", {})
            if response.status_code in {401, 403}:
                raise ActorInvocationError(
                    "unauthenticated", request_id, "application credential rejected"
                )
            if not isinstance(error, dict):
                raise ActorProtocolError("invalid invocation error")
            error = cast(dict[str, Any], error)
            if not isinstance(error.get("code"), str) or not isinstance(error.get("message"), str):
                raise ActorProtocolError(f"invalid HTTP {response.status_code} error")
            raise ActorInvocationError(error["code"], request_id, error["message"])
        if target is not None:
            return value
        target = validate_target(value.get("target"))
        reply = value.get("outcome")
        if not isinstance(reply, dict):
            raise ActorProtocolError("missing invocation outcome")
        reply = cast(dict[str, Any], reply)
        if reply.get("type") in {"completed", "failed"}:
            self._targets[key] = target
        return reply


def component(value: str, maximum: int) -> str:
    if not re.fullmatch(r"[A-Za-z0-9._-]+", value) or value in {".", ".."} or len(value) > maximum:
        raise ValueError("invalid actor identifier")
    return value


def validate_origin(value: str) -> str:
    parsed = urlsplit(value)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("expected an HTTP or HTTPS origin")
    return value.rstrip("/")


def validate_target(value: Any) -> dict[str, Any]:
    if not is_document(value):
        raise ActorProtocolError("invalid actor target")
    if not isinstance(value.get("token"), str) or not value["token"].strip():
        raise ActorProtocolError("invalid actor target")
    for key in ("ownerEpoch", "expiresAtMs"):
        if type(value.get(key)) is not int or not 0 < value[key] <= 2**53 - 1:
            raise ActorProtocolError("invalid actor target")
    validate_origin(value["route"])
    return value


def document(response: httpx.Response) -> dict[str, Any]:
    try:
        value = response.json()
    except ValueError as error:
        raise ActorProtocolError("response was not JSON") from error
    if not is_document(value):
        raise ActorProtocolError("response must be a JSON object")
    return value


def refused(error: BaseException) -> bool:
    import errno

    seen: set[int] = set()
    while id(error) not in seen:
        seen.add(id(error))
        if isinstance(error, OSError) and error.errno == errno.ECONNREFUSED:
            return True
        cause = error.__cause__ or error.__context__
        if cause is None:
            return False
        error = cause
    return False
