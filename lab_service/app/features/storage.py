"""Segments on disk, retention, deleting records and log compaction."""

import asyncio
from aiokafka.admin import RecordsToDelete
from aiokafka.structs import TopicPartition
from app import kit
from app.kit import Report, admin, consumer, disk_batches, disk_files, producer, read_topic, topic
from app.registry import feature

GROUP = "Storage and retention"


async def first_offset(name: str, partition: int = 0) -> int:
    tp = TopicPartition(name, partition)
    async with consumer() as c:
        return (await c.beginning_offsets([tp]))[tp]


@feature(
    "segments", GROUP, "Segments: the real files on disk",
    "A partition is a folder of segment files. Each .log file is named after the first offset it holds.",
    "Old data leaves by deleting whole files, which is cheap. The list below is read from broker 1's disk.",
    "ride-events-1/00000000000000000000.log",
)
async def segments(r: Report) -> None:
    async with topic("segments", configs={"segment.ms": "1000"}) as name, producer(acks="all") as p:
        for round_number in range(3):
            for i in range(50):
                await p.send_and_wait(name, value=kit.dumps({"round": round_number, "i": i}))
            await asyncio.sleep(1.2)
        await p.send_and_wait(name, value=b"last")

        async def rolled():
            return sum(f.endswith(".log") for f, _ in disk_files(name)) >= 3
        await kit.wait_until(rolled, what="new segments")
        files = disk_files(name)
        r.table(
            ["File", "Bytes"],
            [[f, f"{size:,}"] for f, size in files if f.endswith((".log", ".index", ".timeindex"))],
            f"{name}-0 on broker 1",
        )
        logs = [f for f, _ in files if f.endswith(".log")]
        r.check("The partition rolled into several segments", len(logs) >= 3, len(logs))
        r.check("Each file is named after its first offset", logs[0].startswith("0" * 20), logs[0])
        r.check("An index sits beside each log", any(f.endswith(".index") for f, _ in files))


@feature(
    "retention", GROUP, "Retention: old data is deleted by age",
    "Each topic keeps data for a set time, then removes the oldest segments. The default is 7 days.",
    "Kafka does not delete on read. Retention is what frees the disk. This topic keeps data for 1 second.",
    '{"retention.ms": "1000", "segment.ms": "1000"}',
)
async def retention(r: Report) -> None:
    configs = {"retention.ms": "1000", "segment.ms": "1000"}
    async with topic("retention", configs=configs) as name, producer(acks="all") as p:
        for i in range(10):
            await p.send_and_wait(name, value=kit.dumps({"n": i}))
        r.step("Wrote 10 messages. Waiting for the broker's cleanup to run.")

        async def cleaned():
            await p.send_and_wait(name, value=b"keep the log rolling")
            return await first_offset(name) >= 10
        await kit.wait_until(cleaned, timeout=45, every=1.0, what="retention to delete old data")
        start = await first_offset(name)
        r.check("The oldest offset moved forward: old messages are gone", start >= 10, f"first offset is now {start}")


@feature(
    "delete-records", GROUP, "Delete records up to an offset",
    "The Admin API can remove everything before an offset right away, without waiting for retention.",
    "Useful for clearing bad data, or a topic used as a temporary buffer.",
    "await admin.delete_records({tp: RecordsToDelete(before_offset=5)})",
)
async def delete_records(r: Report) -> None:
    async with topic("delete") as name, producer(acks="all") as p:
        for i in range(10):
            await p.send_and_wait(name, value=kit.dumps({"n": i}))
        tp = TopicPartition(name, 0)
        async with admin() as a:
            result = await a.delete_records({tp: RecordsToDelete(before_offset=5)})
        r.step(f"Kafka reports the new first offset as {result[tp]}.")
        remaining = await read_topic(name)
        r.check("Offsets 0 to 4 are gone", [m.offset for m in remaining] == list(range(5, 10)), [m.offset for m in remaining])


@feature(
    "compaction", GROUP, "Log compaction",
    "A compacted topic keeps the newest value of each key, however old. A null value deletes the key.",
    "Perfect for current state, such as each driver's status. The topic works like a table.",
    '{"cleanup.policy": "compact"}',
)
async def compaction(r: Report) -> None:
    configs = {
        "cleanup.policy": "compact",
        "segment.ms": "500",
        "min.cleanable.dirty.ratio": "0.001",
        "delete.retention.ms": "100",
    }
    async with topic("compact", configs=configs) as name, producer(acks="all") as p:
        for version in range(1, 6):
            for key in (b"DRV-1", b"DRV-2", b"DRV-3"):
                await p.send_and_wait(name, key=key, value=kit.dumps({"version": version}))
        await p.send_and_wait(name, key=b"DRV-3", value=None)
        before = len(await read_topic(name))
        r.step(f"Wrote {before} messages: 5 versions of 3 drivers, then deleted DRV-3 with a null value.")

        async def compacted():
            await asyncio.sleep(0.6)
            await p.send_and_wait(name, key=b"filler", value=b"closes the active segment")
            rows = await read_topic(name)
            keys = [m.key for m in rows if m.key != b"filler"]
            return rows if sorted(keys) == [b"DRV-1", b"DRV-2"] else None

        rows = await kit.wait_until(compacted, timeout=60, every=0.5, what="the log cleaner")
        current = {m.key.decode(): kit.loads(m.value)["version"] for m in rows if m.key != b"filler"}
        r.table(["Key", "Version kept"], sorted(current.items()), f"After compaction ({before} messages became {len(rows)})")
        r.check("Only the newest value of each key survived", current == {"DRV-1": 5, "DRV-2": 5}, current)
        r.check("The null value removed DRV-3 completely", "DRV-3" not in current)
        offsets = [m.offset for m in rows]
        r.check("Offsets kept their numbers, leaving gaps", offsets[0] > 0, offsets)


@feature(
    "transaction-markers", GROUP, "Control records in the log",
    "A transaction writes an invisible commit or abort marker into the log. It takes up one offset.",
    "This explains the small gaps you may see in the offsets of a transactional topic.",
    "# shown from the real .log file",
)
async def transaction_markers(r: Report) -> None:
    async with topic("markers") as name, producer(transactional_id=kit.unique("tx")) as p:
        async with p.transaction():
            await p.send_and_wait(name, value=b"one")
            await p.send_and_wait(name, value=b"two")

        await kit.wait_until(lambda: any(b["control"] for b in disk_batches(name)), what="the marker on disk")
        batches = disk_batches(name)
        r.table(
            ["Base offset", "Records", "Transactional", "Control marker"],
            [[b["base_offset"], b["records"], b["transactional"], b["control"]] for b in batches],
            "Batches parsed from the .log file",
        )
        visible = await read_topic(name, isolation_level="read_committed")
        r.check("Readers see 2 messages", len(visible) == 2, len(visible))
        r.check("The file also holds a control marker", any(b["control"] for b in batches))
