"""Print actor inventory and persisted snapshots from the control plane."""

from __future__ import annotations

import json
from typing import Any, cast
from urllib.parse import urlencode

import httpx

from .client import ActorProtocolError, Client, component, document
from .guards import is_document, is_list

INCOMPLETE_CONNECTIONS = "Warning: connection inventory is incomplete."


def run_inspect(
    origin: str | None, selector: str | None, json_output: bool
) -> tuple[str, str | None]:
    with Client(origin) as client:
        inventory = parse_inventory(read_json(client, "observe/actors"))
        if selector is None:
            return inspect_actors(inventory, json_output=json_output)
        parsed = parse_selector(selector)
        if find_instance(inventory, parsed) is None:
            raise ValueError(f"No actor instance {parsed[0]}/{parsed[1]}.")
        state = parse_state(
            read_json(
                client,
                "observe/state?" + urlencode({"actorName": parsed[0], "actorId": parsed[1]}),
            )
        )
        return inspect_actors(inventory, selector=selector, state=state, json_output=json_output)


def inspect_actors(
    inventory: dict[str, Any],
    *,
    selector: str | None = None,
    state: dict[str, Any] | None = None,
    json_output: bool = False,
) -> tuple[str, str | None]:
    inventory = parse_inventory(inventory)
    warning = None if inventory["connectionsComplete"] else INCOMPLETE_CONNECTIONS
    if selector is None:
        text = json.dumps(inventory, indent=2) if json_output else format_inventory(inventory)
        return text, warning
    parsed = parse_selector(selector)
    instance = find_instance(inventory, parsed)
    if instance is None:
        raise ValueError(f"No actor instance {parsed[0]}/{parsed[1]}.")
    if state is None:
        raise ValueError(f"No persisted snapshot for {parsed[0]}/{parsed[1]}.")
    state = parse_state(state)
    if json_output:
        text = json.dumps(
            {"actorName": parsed[0], "actorId": parsed[1], "instance": instance, "state": state},
            indent=2,
        )
    else:
        text = format_instance(parsed, instance, state)
    return text, warning


def parse_selector(value: str) -> tuple[str, str]:
    actor_name, separator, actor_id = value.partition("/")
    if not separator or "/" in actor_id:
        raise ValueError("Expected ActorName/actorId.")
    try:
        return component(actor_name, 255), component(actor_id, 128)
    except ValueError:
        raise ValueError("Expected ActorName/actorId.") from None


def format_inventory(inventory: dict[str, Any]) -> str:
    actors = inventory["actors"]
    if not actors:
        return "No actor instances."
    return "\n".join(format_class(row) for row in actors)


def format_instance(
    selector: tuple[str, str], instance: dict[str, Any], state: dict[str, Any]
) -> str:
    actor_name, actor_id = selector
    lines = [f"{actor_name}/{actor_id}", f"status: {instance['status']}"]
    connections = instance.get("connections") or []
    if connections:
        lines.append(f"connections: {len(connections)}")
        lines.extend(format_connection(connection) for connection in connections)
    waiting = instance.get("waiting") or []
    if waiting:
        lines.append("waiting: " + ", ".join(item["operation"] for item in waiting))
    snapshot = state.get("snapshot")
    if not snapshot:
        lines.append(f"No persisted snapshot for {actor_name}/{actor_id}.")
        return "\n".join(lines)
    lines.append(f"stateVersion: {snapshot['stateVersion']}")
    lines.append(f"requestId: {snapshot['requestId']}")
    attribution = snapshot.get("attribution")
    if is_document(attribution) and isinstance(attribution.get("operation"), str):
        lines.append(f"lastWrite: {attribution['operation']}")
    lines.append(json.dumps(snapshot["state"], indent=2))
    return "\n".join(lines)


def format_class(row: dict[str, Any]) -> str:
    header = f"{row['actorName']}  live={row['live']}  dormant={row['dormant']}  unknown={row['unknown']}"
    instances = [format_instance_line(instance) for instance in row["instances"]]
    return "\n".join([header, *instances])


def format_instance_line(instance: dict[str, Any]) -> str:
    details: list[str] = []
    connections = instance.get("connections") or []
    if connections:
        details.append(f"connections={len(connections)}")
    waiting = instance.get("waiting") or []
    if waiting:
        details.append("waiting=" + ",".join(item["operation"] for item in waiting))
    suffix = f"  {'  '.join(details)}" if details else ""
    return f"  {instance['actorId']}  {instance['status']}{suffix}"


def format_connection(connection: dict[str, Any]) -> str:
    metadata = connection.get("metadata")
    if metadata is None:
        return f"  {connection['id']}"
    return f"  {connection['id']}  {json.dumps(metadata)}"


def find_instance(inventory: dict[str, Any], selector: tuple[str, str]) -> dict[str, Any] | None:
    actor_name, actor_id = selector
    for row in inventory["actors"]:
        if row["actorName"] != actor_name:
            continue
        for instance in row["instances"]:
            if instance["actorId"] == actor_id:
                return cast(dict[str, Any], instance)
    return None


def parse_inventory(value: object) -> dict[str, Any]:
    if (
        not is_document(value)
        or not isinstance(value.get("connectionsComplete"), bool)
        or not is_list(value.get("actors"))
    ):
        raise ValueError("Invalid actor inventory response.")
    for row in value["actors"]:
        if (
            not is_document(row)
            or not isinstance(row.get("actorName"), str)
            or not is_list(row.get("instances"))
            or not all(
                isinstance(row.get(key), int) and row[key] >= 0
                for key in ("live", "dormant", "unknown")
            )
        ):
            raise ValueError("Invalid actor inventory response.")
        for instance in row["instances"]:
            if not is_instance(instance):
                raise ValueError("Invalid actor inventory response.")
    return value


def parse_state(value: object) -> dict[str, Any]:
    if not is_document(value):
        raise ValueError("Invalid state response.")
    snapshot = value.get("snapshot")
    if snapshot is not None and not (is_document(snapshot) and is_document(snapshot.get("state"))):
        raise ValueError("Invalid state response.")
    schema = value.get("schema")
    if schema is not None and not is_document(schema):
        raise ValueError("Invalid state response.")
    return value


def is_instance(value: object) -> bool:
    if (
        not is_document(value)
        or not isinstance(value.get("actorId"), str)
        or value.get("status") not in {"live", "dormant", "unknown"}
        or not is_list(value.get("connections"))
    ):
        return False
    waiting = value.get("waiting")
    if waiting is not None and not (
        is_list(waiting)
        and all(
            is_document(item)
            and isinstance(item.get("id"), str)
            and isinstance(item.get("operation"), str)
            for item in waiting
        )
    ):
        return False
    return all(
        is_document(connection) and isinstance(connection.get("id"), str)
        for connection in value["connections"]
    )


def read_json(client: Client, path: str) -> dict[str, Any]:
    response = httpx.get(
        f"{client.origin}/v1/projects/{client.project_id}/{path}",
        headers=client.headers,
        follow_redirects=False,
        timeout=30.0,
    )
    response.raise_for_status()
    try:
        return document(response)
    except ActorProtocolError as error:
        raise ValueError("Invalid inspect response.") from error
