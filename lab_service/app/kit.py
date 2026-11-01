"""The small tools every feature shares.

Each feature gets its own throwaway topic, talks to the real cluster, and writes
down what really happened in a Report. Nothing here is simulated.
"""

import asyncio
import inspect
import json
import struct
import time
import uuid
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.structs import TopicPartition
from app import config


class Skip(Exception):
    """This feature needs a part of the stack that is not running."""


class Report:
    """What really happened, in order: steps, tables and pass/fail checks."""

    def __init__(self, options: dict | None = None) -> None:
        self.items: list[dict] = []
        self.options = options or {}

    def step(self, text: str) -> None:
        self.items.append({"kind": "step", "text": text})

    def table(self, headers: list[str], rows: list[list[Any]], title: str = "") -> None:
        self.items.append({"kind": "table", "title": title, "headers": headers, "rows": rows})

    def check(self, label: str, ok: Any, detail: Any = "") -> bool:
        self.items.append({"kind": "check", "label": label, "ok": bool(ok), "detail": str(detail)})
        return bool(ok)

    @property
    def passed(self) -> bool:
        checks = [i for i in self.items if i["kind"] == "check"]
        return bool(checks) and all(c["ok"] for c in checks)


def unique(hint: str) -> str:
    return f"lab.{hint}.{uuid.uuid4().hex[:8]}"


def dumps(value: Any) -> bytes:
    return json.dumps(value).encode("utf-8")


def loads(raw: bytes) -> Any:
    return json.loads(raw)


async def wait_until(
    condition: Callable[[], Any | Awaitable[Any]],
    timeout: float = 20.0,
    every: float = 0.2,
    what: str = "condition",
) -> Any:
    """Poll until condition() is truthy, and return that value."""
    deadline = time.monotonic() + timeout
    while True:
        value = condition()
        if inspect.isawaitable(value):
            value = await value
        if value:
            return value
        if time.monotonic() > deadline:
            raise TimeoutError(f"timed out after {timeout:.0f}s waiting for {what}")
        await asyncio.sleep(every)


@asynccontextmanager
async def admin():
    client = AIOKafkaAdminClient(bootstrap_servers=config.BOOTSTRAP, client_id="lab-admin")
    await client.start()
    try:
        yield client
    finally:
        await client.close()


async def _topic_ready(client: AIOKafkaAdminClient, name: str, partitions: int) -> bool:
    try:
        info = (await client.describe_topics([name]))[0]
    except Exception:
        return False
    found = info.get("partitions") or []
    return len(found) == partitions and all(p["leader"] >= 0 for p in found)


@asynccontextmanager
async def topic(
    hint: str,
    partitions: int = 1,
    replicas: int = 3,
    configs: dict[str, str] | None = None,
):
    """Create a throwaway topic, wait until it has leaders, and clean up afterwards."""
    name = unique(hint)
    async with admin() as client:
        await client.create_topics([NewTopic(name, partitions, replicas, topic_configs=configs or {})])
        await wait_until(lambda: _topic_ready(client, name, partitions), what=f"topic {name}")
    try:
        yield name
    finally:
        try:
            async with admin() as client:
                await client.delete_topics([name])
        except Exception:
            pass  # a leftover test topic is harmless


@asynccontextmanager
async def producer(**options):
    options.setdefault("bootstrap_servers", config.BOOTSTRAP)
    # With acks=1 a reader can arrive before the data is replicated, and see nothing yet.
    options.setdefault("acks", "all")
    client = AIOKafkaProducer(**options)
    await client.start()
    try:
        yield client
    finally:
        await client.stop()


@asynccontextmanager
async def consumer(*topics: str, **options):
    options.setdefault("bootstrap_servers", config.BOOTSTRAP)
    options.setdefault("auto_offset_reset", "earliest")
    options.setdefault("enable_auto_commit", False)
    client = AIOKafkaConsumer(*topics, **options)
    await client.start()
    try:
        yield client
    finally:
        await client.stop()


async def read(client: AIOKafkaConsumer, want: int | None = None, quiet_ms: int = 1000, limit: float = 20.0) -> list:
    """Read until `want` messages arrived, or (with no want) until the topic goes quiet."""
    received: list = []
    deadline = time.monotonic() + limit
    while time.monotonic() < deadline:
        batches = await client.getmany(timeout_ms=quiet_ms)
        fresh = [m for messages in batches.values() for m in messages]
        received.extend(fresh)
        if want is not None and len(received) >= want:
            break
        if want is None and not fresh:
            break
    return received


async def read_topic(name: str, partitions: int = 1, limit: float = 15.0, **options) -> list:
    """Read everything currently in a topic, from a consumer with no group."""
    async with consumer(**options) as client:
        tps = [TopicPartition(name, p) for p in range(partitions)]
        client.assign(tps)
        await client.seek_to_beginning(*tps)
        ends = await client.end_offsets(tps)
        begins = await client.beginning_offsets(tps)
        received: list = []
        deadline = time.monotonic() + limit
        while time.monotonic() < deadline:
            positions = {tp: await client.position(tp) for tp in tps}
            if all(positions[tp] >= ends[tp] for tp in tps) or all(ends[tp] == begins[tp] for tp in tps):
                break
            batches = await client.getmany(timeout_ms=500)
            received.extend(m for messages in batches.values() for m in messages)
        return received


async def group_assignment(client: AIOKafkaConsumer) -> list[int]:
    return sorted(tp.partition for tp in client.assignment())


CODECS = {0: "none", 1: "gzip", 2: "snappy", 3: "lz4", 4: "zstd"}


def partition_dir(name: str, partition: int = 0):
    return config.BROKER_DATA / f"{name}-{partition}"


def disk_files(name: str, partition: int = 0) -> list[tuple[str, int]]:
    """The real files one partition has on broker 1: (file name, bytes)."""
    folder = partition_dir(name, partition)
    if not folder.exists():
        return []
    return sorted((f.name, f.stat().st_size) for f in folder.iterdir() if f.is_file())


def disk_batches(name: str, partition: int = 0) -> list[dict]:
    """Parse the record batches straight out of the .log files.

    The layout comes from the Kafka protocol: base offset (8 bytes), batch length
    (4), then a header holding the attributes at byte 21 and record count at 57.
    """
    found: list[dict] = []
    folder = partition_dir(name, partition)
    if not folder.exists():
        return found
    for path in sorted(folder.glob("*.log")):
        data = path.read_bytes()
        pos = 0
        while pos + 61 <= len(data):
            base_offset, length = struct.unpack_from(">qi", data, pos)
            if length <= 0:
                break
            attributes = struct.unpack_from(">h", data, pos + 21)[0]
            records = struct.unpack_from(">i", data, pos + 57)[0]
            found.append(
                {
                    "base_offset": base_offset,
                    "records": records,
                    "bytes": 12 + length,
                    "codec": CODECS.get(attributes & 0x07, "?"),
                    "transactional": bool(attributes & 0x10),
                    "control": bool(attributes & 0x20),
                }
            )
            pos += 12 + length
    return found
