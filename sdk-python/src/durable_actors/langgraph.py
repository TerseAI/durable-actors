"""LangGraph checkpoint saver backed by isolated Durable Actors."""

from __future__ import annotations

import base64
import builtins
import inspect
import random
from collections.abc import AsyncIterator, Callable, Iterator, Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Any, Self, cast

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.serde.base import SerializerProtocol

from .actor import Actor
from .fields import persisted
from .json import JsonValue

if TYPE_CHECKING:
    from .async_client import AsyncRpcTransport
    from .client import RpcTransport

try:
    from langgraph.checkpoint.base import (
        WRITES_IDX_MAP,
        BaseCheckpointSaver,
        ChannelVersions,
        Checkpoint,
        CheckpointMetadata,
        CheckpointTuple,
        get_checkpoint_id,
        get_checkpoint_metadata,
        writes_sort_key,
    )
except ImportError as error:
    raise ImportError(
        "langgraph-checkpoint is required to use DurableActorSaver. "
        "Install it with 'pip install durable-actors[langgraph]' or 'pip install langgraph-checkpoint'."
    ) from error


class LangGraphCheckpointActor(Actor[JsonValue, JsonValue, JsonValue, str]):
    """Durable Actor managing checkpoint state and pending writes for a LangGraph thread."""

    checkpoints: dict[str, dict[str, JsonValue]] = persisted(
        default_factory=cast(Callable[[], dict[str, dict[str, JsonValue]]], dict)
    )
    writes: dict[str, list[dict[str, JsonValue]]] = persisted(
        default_factory=cast(Callable[[], dict[str, list[dict[str, JsonValue]]]], dict)
    )
    blobs: dict[str, list[str]] = persisted(
        default_factory=cast(Callable[[], dict[str, list[str]]], dict)
    )

    def put_checkpoint(
        self,
        checkpoint_ns: str,
        checkpoint_id: str,
        parent_checkpoint_id: str | None,
        checkpoint_blob: list[JsonValue],
        metadata_blob: list[JsonValue],
        channel_versions: dict[str, JsonValue],
        new_blobs: dict[str, list[str]],
    ) -> None:
        """Persist a checkpoint snapshot and new channel blobs."""
        key = f"{checkpoint_ns}:{checkpoint_id}"
        self.checkpoints[key] = {
            "checkpoint_id": checkpoint_id,
            "checkpoint_ns": checkpoint_ns,
            "parent_checkpoint_id": parent_checkpoint_id,
            "checkpoint_blob": checkpoint_blob,
            "metadata_blob": metadata_blob,
            "channel_versions": channel_versions,
        }
        self.blobs.update(new_blobs)
        self._safe_broadcast(
            {
                "type": "checkpoint_saved",
                "checkpoint_id": checkpoint_id,
                "checkpoint_ns": checkpoint_ns,
            }
        )

    def put_writes(
        self,
        checkpoint_ns: str,
        checkpoint_id: str,
        writes: list[dict[str, JsonValue]],
    ) -> None:
        """Store pending writes for a super-step idempotently."""
        key = f"{checkpoint_ns}:{checkpoint_id}"
        if key not in self.writes:
            self.writes[key] = []
        for w in writes:
            task_id = w["task_id"]
            idx = w["idx"]
            existing = False
            for current in self.writes[key]:
                current_idx = current.get("idx")
                if (
                    current["task_id"] == task_id
                    and current_idx == idx
                    and isinstance(idx, int)
                    and idx >= 0
                ):
                    existing = True
                    break
            if not existing:
                self.writes[key].append(w)
        self._safe_broadcast(
            {
                "type": "writes_saved",
                "checkpoint_id": checkpoint_id,
                "count": len(writes),
            }
        )

    def get_checkpoint(
        self, checkpoint_ns: str = "", checkpoint_id: str | None = None
    ) -> dict[str, JsonValue] | None:
        """Retrieve a specific checkpoint or the latest for a namespace."""
        record = self._resolve_record(checkpoint_ns, checkpoint_id)
        if record is None:
            return None
        return self._format_record(record, checkpoint_ns)

    def list_checkpoints(
        self,
        checkpoint_ns: str | None = None,
        checkpoint_id: str | None = None,
        before_checkpoint_id: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, JsonValue]]:
        """List checkpoints matching filtering criteria ordered by checkpoint_id descending."""
        if limit is not None and limit <= 0:
            return []
        candidates = [
            r
            for r in self.checkpoints.values()
            if (checkpoint_ns is None or r["checkpoint_ns"] == checkpoint_ns)
            and (checkpoint_id is None or r["checkpoint_id"] == checkpoint_id)
            and (
                before_checkpoint_id is None
                or str(r.get("checkpoint_id", "")) < before_checkpoint_id
            )
        ]
        candidates.sort(key=lambda r: str(r["checkpoint_id"]), reverse=True)
        if limit is not None:
            candidates = candidates[:limit]
        return [self._format_record(r, str(r["checkpoint_ns"])) for r in candidates]

    def delete_thread(self) -> None:
        """Clear all stored state for this actor thread."""
        self.checkpoints.clear()
        self.writes.clear()
        self.blobs.clear()
        self._safe_broadcast({"type": "thread_deleted"})

    def _safe_broadcast(self, message: JsonValue) -> None:
        try:
            self.broadcast(message)
        except (LookupError, ValueError, RuntimeError):
            pass

    def _resolve_record(
        self, checkpoint_ns: str, checkpoint_id: str | None
    ) -> dict[str, JsonValue] | None:
        if checkpoint_id:
            return self.checkpoints.get(f"{checkpoint_ns}:{checkpoint_id}")
        matching = [r for r in self.checkpoints.values() if r["checkpoint_ns"] == checkpoint_ns]
        if not matching:
            return None
        return max(matching, key=lambda r: str(r["checkpoint_id"]))

    def _format_record(
        self, record: dict[str, JsonValue], checkpoint_ns: str
    ) -> dict[str, JsonValue]:
        required_blobs: dict[str, list[str]] = {}
        channel_versions = record.get("channel_versions")
        if isinstance(channel_versions, dict):
            for ch, ver in channel_versions.items():
                blob_key = f"{checkpoint_ns}:{ch}:{ver}"
                if blob_key in self.blobs:
                    required_blobs[ch] = self.blobs[blob_key]
        key = f"{checkpoint_ns}:{record['checkpoint_id']}"
        stored_writes = self.writes.get(key, [])
        sorted_writes = sorted(
            stored_writes,
            key=lambda w: writes_sort_key(
                str(w.get("task_path", "")), str(w.get("task_id", "")), int(str(w.get("idx", 0)))
            ),
        )
        formatted_writes = [
            [w["task_id"], w["channel"], w["value"], w.get("task_path", "")] for w in sorted_writes
        ]
        return {
            "checkpoint_id": record["checkpoint_id"],
            "checkpoint_ns": record["checkpoint_ns"],
            "parent_checkpoint_id": record["parent_checkpoint_id"],
            "checkpoint_blob": record["checkpoint_blob"],
            "metadata_blob": record["metadata_blob"],
            "blobs": cast(JsonValue, required_blobs),
            "writes": cast(JsonValue, formatted_writes),
        }


class DurableActorSaver(BaseCheckpointSaver[str]):
    """LangGraph checkpointer that delegates each thread_id to an isolated Durable Actor."""

    def __init__(
        self,
        *,
        transport: RpcTransport | None = None,
        async_transport: AsyncRpcTransport | None = None,
        actor_name: str = "LangGraphCheckpointActor",
        serde: SerializerProtocol | None = None,
    ) -> None:
        super().__init__(serde=serde)
        self.actor_name = actor_name
        self._transport = transport
        self._async_transport = async_transport
        self._known_thread_ids: set[str] = set()

    @classmethod
    def create_in_memory(
        cls,
        *,
        transport: Any | None = None,
        actor_name: str = "LangGraphCheckpointActor",
        serde: SerializerProtocol | None = None,
    ) -> Self:
        """Create a checkpointer with an in-memory transport for local testing."""
        from .testing import InMemoryActorTransport

        mem = (
            transport if isinstance(transport, InMemoryActorTransport) else InMemoryActorTransport()
        )
        return cls(
            transport=mem,
            async_transport=mem.as_async(),
            actor_name=actor_name,
            serde=serde,
        )

    @property
    def transport(self) -> RpcTransport:
        if self._transport is None:
            from .client import default_client

            self._transport = default_client()
        return self._transport

    @property
    def async_transport(self) -> AsyncRpcTransport:
        if self._async_transport is None:
            from .async_client import default_async_client

            self._async_transport = default_async_client()
        return self._async_transport

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool | None:
        return None

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool | None:
        return None

    def get_next_version(self, current: str | None, channel: None) -> str:
        """Generate monotonically increasing string version IDs."""
        if current is None:
            current_v = 0
        elif isinstance(current, int):
            current_v = current
        else:
            current_v = int(current.split(".")[0])
        next_v = current_v + 1
        next_h = random.random()
        return f"{next_v:032}.{next_h:016}"

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """Store a checkpoint snapshot in the thread's isolated actor."""
        configurable = config.get("configurable") or {}
        thread_id = configurable.get("thread_id")
        if not thread_id:
            raise ValueError("RunnableConfig missing required 'thread_id' in configurable")
        checkpoint_ns = cast(str, configurable.get("checkpoint_ns", ""))
        parent_checkpoint_id = cast(str | None, configurable.get("checkpoint_id"))
        self._known_thread_ids.add(thread_id)

        c = checkpoint.copy()
        values: dict[str, Any] = c.pop("channel_values")  # type: ignore[misc]
        new_blobs: dict[str, list[str]] = {}
        for k, v in new_versions.items():
            blob = self.serde.dumps_typed(values[k]) if k in values else ("empty", b"")
            new_blobs[f"{checkpoint_ns}:{k}:{v}"] = [
                blob[0],
                base64.b64encode(blob[1]).decode("ascii"),
            ]

        dumped_c = self.serde.dumps_typed(c)
        encoded_c = [dumped_c[0], base64.b64encode(dumped_c[1]).decode("ascii")]

        dumped_m = self.serde.dumps_typed(get_checkpoint_metadata(config, metadata))
        encoded_m = [dumped_m[0], base64.b64encode(dumped_m[1]).decode("ascii")]

        self.transport.invoke(
            self.actor_name,
            thread_id,
            "put_checkpoint",
            [
                checkpoint_ns,
                checkpoint["id"],
                parent_checkpoint_id,
                encoded_c,
                encoded_m,
                c["channel_versions"],
                new_blobs,
            ],
        )
        return {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint["id"],
            }
        }

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """Asynchronously store a checkpoint snapshot in the thread's isolated actor."""
        configurable = config.get("configurable") or {}
        thread_id = configurable.get("thread_id")
        if not thread_id:
            raise ValueError("RunnableConfig missing required 'thread_id' in configurable")
        checkpoint_ns = cast(str, configurable.get("checkpoint_ns", ""))
        parent_checkpoint_id = cast(str | None, configurable.get("checkpoint_id"))
        self._known_thread_ids.add(thread_id)

        c = checkpoint.copy()
        values: dict[str, Any] = c.pop("channel_values")  # type: ignore[misc]
        new_blobs: dict[str, list[str]] = {}
        for k, v in new_versions.items():
            blob = self.serde.dumps_typed(values[k]) if k in values else ("empty", b"")
            new_blobs[f"{checkpoint_ns}:{k}:{v}"] = [
                blob[0],
                base64.b64encode(blob[1]).decode("ascii"),
            ]

        dumped_c = self.serde.dumps_typed(c)
        encoded_c = [dumped_c[0], base64.b64encode(dumped_c[1]).decode("ascii")]

        dumped_m = self.serde.dumps_typed(get_checkpoint_metadata(config, metadata))
        encoded_m = [dumped_m[0], base64.b64encode(dumped_m[1]).decode("ascii")]

        await self.async_transport.invoke(
            self.actor_name,
            thread_id,
            "put_checkpoint",
            [
                checkpoint_ns,
                checkpoint["id"],
                parent_checkpoint_id,
                encoded_c,
                encoded_m,
                c["channel_versions"],
                new_blobs,
            ],
        )
        return {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint["id"],
            }
        }

    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        """Retrieve a checkpoint tuple for the configured thread."""
        configurable = config.get("configurable") or {}
        thread_id = configurable.get("thread_id")
        if not thread_id:
            return None
        checkpoint_ns = cast(str, configurable.get("checkpoint_ns", ""))
        checkpoint_id = get_checkpoint_id(config)
        self._known_thread_ids.add(thread_id)

        record = self.transport.invoke(
            self.actor_name,
            thread_id,
            "get_checkpoint",
            [checkpoint_ns, checkpoint_id],
        )
        if record is None:
            return None
        return self._build_checkpoint_tuple(thread_id, record)

    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        """Asynchronously retrieve a checkpoint tuple for the configured thread."""
        configurable = config.get("configurable") or {}
        thread_id = configurable.get("thread_id")
        if not thread_id:
            return None
        checkpoint_ns = cast(str, configurable.get("checkpoint_ns", ""))
        checkpoint_id = get_checkpoint_id(config)
        self._known_thread_ids.add(thread_id)

        record = await self.async_transport.invoke(
            self.actor_name,
            thread_id,
            "get_checkpoint",
            [checkpoint_ns, checkpoint_id],
        )
        if record is None:
            return None
        return self._build_checkpoint_tuple(thread_id, record)

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """Store pending writes for an active super-step."""
        configurable = config.get("configurable") or {}
        thread_id = configurable.get("thread_id")
        if not thread_id:
            raise ValueError("RunnableConfig missing required 'thread_id' in configurable")
        checkpoint_id = configurable.get("checkpoint_id")
        if not checkpoint_id:
            raise ValueError("RunnableConfig missing required 'checkpoint_id' in configurable")
        checkpoint_ns = cast(str, configurable.get("checkpoint_ns", ""))
        self._known_thread_ids.add(thread_id)

        encoded_writes = self._encode_writes(writes, task_id, task_path)
        self.transport.invoke(
            self.actor_name,
            thread_id,
            "put_writes",
            [checkpoint_ns, checkpoint_id, encoded_writes],
        )

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """Asynchronously store pending writes for an active super-step."""
        configurable = config.get("configurable") or {}
        thread_id = configurable.get("thread_id")
        if not thread_id:
            raise ValueError("RunnableConfig missing required 'thread_id' in configurable")
        checkpoint_id = configurable.get("checkpoint_id")
        if not checkpoint_id:
            raise ValueError("RunnableConfig missing required 'checkpoint_id' in configurable")
        checkpoint_ns = cast(str, configurable.get("checkpoint_ns", ""))
        self._known_thread_ids.add(thread_id)

        encoded_writes = self._encode_writes(writes, task_id, task_path)
        await self.async_transport.invoke(
            self.actor_name,
            thread_id,
            "put_writes",
            [checkpoint_ns, checkpoint_id, encoded_writes],
        )

    def list(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> Iterator[CheckpointTuple]:
        """List checkpoints matching the filter criteria."""
        if limit is not None and limit <= 0:
            return

        configurable = config.get("configurable") if config else None
        thread_id = configurable.get("thread_id") if configurable else None
        if thread_id:
            thread_ids = [thread_id]
        elif hasattr(self.transport, "list_actors"):
            discovered = cast(list[str], getattr(self.transport, "list_actors")(self.actor_name))
            thread_ids = list(dict.fromkeys(list(self._known_thread_ids) + discovered))
        else:
            thread_ids = list(self._known_thread_ids)

        c_ns = configurable.get("checkpoint_ns") if configurable else None
        c_id = get_checkpoint_id(config) if config else None
        before_c_id = get_checkpoint_id(before) if before else None

        actor_limit = None if filter else limit
        count = 0
        for tid in thread_ids:
            records = self.transport.invoke(
                self.actor_name,
                tid,
                "list_checkpoints",
                [c_ns, c_id, before_c_id, actor_limit],
            )
            for rec in records:
                tup = self._build_checkpoint_tuple(tid, rec)
                if filter and not all(tup.metadata.get(k) == v for k, v in filter.items()):
                    continue
                yield tup
                count += 1
                if limit is not None and count >= limit:
                    return

    async def alist(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[CheckpointTuple]:
        """Asynchronously list checkpoints matching the filter criteria."""
        if limit is not None and limit <= 0:
            return

        configurable = config.get("configurable") if config else None
        thread_id = configurable.get("thread_id") if configurable else None
        if thread_id:
            thread_ids = [thread_id]
        elif hasattr(self.async_transport, "list_actors"):
            raw_discovered = getattr(self.async_transport, "list_actors")(self.actor_name)
            if inspect.isawaitable(raw_discovered):
                discovered = cast(list[str], await raw_discovered)
            else:
                discovered = cast(list[str], raw_discovered)
            thread_ids = list(dict.fromkeys(list(self._known_thread_ids) + discovered))
        else:
            thread_ids = list(self._known_thread_ids)

        c_ns = configurable.get("checkpoint_ns") if configurable else None
        c_id = get_checkpoint_id(config) if config else None
        before_c_id = get_checkpoint_id(before) if before else None

        actor_limit = None if filter else limit
        count = 0
        for tid in thread_ids:
            records = await self.async_transport.invoke(
                self.actor_name,
                tid,
                "list_checkpoints",
                [c_ns, c_id, before_c_id, actor_limit],
            )
            for rec in records:
                tup = self._build_checkpoint_tuple(tid, rec)
                if filter and not all(tup.metadata.get(k) == v for k, v in filter.items()):
                    continue
                yield tup
                count += 1
                if limit is not None and count >= limit:
                    return

    def delete_thread(self, thread_id: str) -> None:
        """Delete all checkpoints and writes for a thread."""
        self.transport.invoke(self.actor_name, thread_id, "delete_thread", [])
        self._known_thread_ids.discard(thread_id)

    async def adelete_thread(self, thread_id: str) -> None:
        """Asynchronously delete all checkpoints and writes for a thread."""
        await self.async_transport.invoke(self.actor_name, thread_id, "delete_thread", [])
        self._known_thread_ids.discard(thread_id)

    def _encode_writes(
        self, writes: Sequence[tuple[str, Any]], task_id: str, task_path: str
    ) -> builtins.list[dict[str, Any]]:
        encoded: list[dict[str, Any]] = []
        for idx, (channel, val) in enumerate(writes):
            w_type, w_bytes = self.serde.dumps_typed(val)
            encoded.append(
                {
                    "task_id": task_id,
                    "idx": WRITES_IDX_MAP.get(channel, idx),
                    "channel": channel,
                    "value": [w_type, base64.b64encode(w_bytes).decode("ascii")],
                    "task_path": task_path,
                }
            )
        return encoded

    def _build_checkpoint_tuple(self, thread_id: str, record: dict[str, Any]) -> CheckpointTuple:
        cid: str = record["checkpoint_id"]
        c_ns: str = record["checkpoint_ns"]
        p_id: str | None = record["parent_checkpoint_id"]

        c_type, c_b64 = record["checkpoint_blob"]
        checkpoint_data: Checkpoint = self.serde.loads_typed(
            (c_type, base64.b64decode(c_b64.encode("ascii")))
        )

        m_type, m_b64 = record["metadata_blob"]
        metadata_data: CheckpointMetadata = self.serde.loads_typed(
            (m_type, base64.b64decode(m_b64.encode("ascii")))
        )

        channel_values: dict[str, Any] = {}
        for ch, (b_type, b_b64) in record.get("blobs", {}).items():
            if b_type != "empty":
                channel_values[ch] = self.serde.loads_typed(
                    (b_type, base64.b64decode(b_b64.encode("ascii")))
                )
        checkpoint_data["channel_values"] = channel_values

        pending_writes: list[tuple[str, str, Any]] = []
        for task_id, channel, (w_type, w_b64), _task_path in record.get("writes", []):
            val = self.serde.loads_typed((w_type, base64.b64decode(w_b64.encode("ascii"))))
            pending_writes.append((task_id, channel, val))

        return CheckpointTuple(
            config={
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": c_ns,
                    "checkpoint_id": cid,
                }
            },
            checkpoint=checkpoint_data,
            metadata=metadata_data,
            parent_config=(
                {
                    "configurable": {
                        "thread_id": thread_id,
                        "checkpoint_ns": c_ns,
                        "checkpoint_id": p_id,
                    }
                }
                if p_id
                else None
            ),
            pending_writes=pending_writes,
        )
