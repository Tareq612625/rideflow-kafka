"""How a message gets in: acks, batching, compression, idempotence and quotas."""

import asyncio
import random
import time
from aiokafka.errors import KafkaError, RequestTimedOutError
from app import kit
from app.kit import Report, admin, disk_batches, producer, read_topic, topic
from app.registry import feature

GROUP = "Producers"


def sample_event(i: int) -> bytes:
    return kit.dumps(
        {
            "event_id": f"{i:012d}-5a6f-4e79-93ad-f056ad32a1fe",
            "trip_id": f"TRIP-{random.randint(100000, 999999)}",
            "driver_id": f"DRV-{random.randint(1, 250):04d}",
            "event_type": random.choice(["requested", "accepted", "completed", "cancelled"]),
            "zone": random.choice(["Gulshan", "Dhanmondi", "Uttara", "Mirpur", "Banani"]),
            "vehicle": random.choice(["bike", "cng", "car", "premium"]),
            "distance_km": round(random.uniform(0.8, 24.0), 2),
            "fare": round(random.uniform(45, 900), 2),
            "ts": "2026-09-20T18:40:08.590+00:00",
        }
    )


@feature(
    "acks", GROUP, "acks: 0, 1 and all",
    "acks decides how much confirmation the producer waits for before moving on.",
    "This is the trade between speed and safety. Use all for anything you cannot lose.",
    'AIOKafkaProducer(acks="all")   # or 0, or 1',
)
async def acks(r: Report) -> None:
    count = 300
    rows = []
    offsets_known = {}
    async with topic("acks") as name:
        for level in (0, 1, "all"):
            async with producer(acks=level, linger_ms=0) as p:
                started = time.monotonic()
                last = None
                for _ in range(count):
                    last = await p.send_and_wait(name, value=b"ride")
                elapsed = time.monotonic() - started
            offsets_known[level] = last.offset if last else None
            rows.append(
                [f"acks={level}", f"{elapsed * 1000:.0f} ms", f"{elapsed / count * 1000:.2f} ms",
                 offsets_known[level] if last else "nothing, the broker never replied"]
            )
        r.table(["Setting", f"Time for {count} messages", "Per message", "Offset returned"], rows)
        r.check("acks=0 gets nothing back, because nobody replied", offsets_known[0] is None)
        r.check("acks=1 and acks=all get a real offset", offsets_known[1] >= 0 and offsets_known["all"] >= 0)
        stored = await kit.wait_until(lambda: _count_is(name, count * 3), what="the unconfirmed messages")
        r.check(f"All {count * 3} messages were stored", len(stored) == count * 3, len(stored))


async def _count_is(name: str, want: int) -> list | None:
    stored = await read_topic(name)
    return stored if len(stored) == want else None


@feature(
    "min-insync", GROUP, "min.insync.replicas refuses unsafe writes",
    "lab.min-isr demands 3 in-sync copies. With acks=all, a write is refused if fewer are available.",
    "A refused write is better than a write you wrongly believe is safe. Stop a broker, then run this again.",
    "docker compose stop kafka-2      # then press Run again\n"
    "docker compose start kafka-2     # afterwards",
)
async def min_insync(r: Report) -> None:
    name = "lab.min-isr"
    async with admin() as client:
        partition = (await client.describe_topics([name]))[0]["partitions"][0]
    in_sync = len(partition["isr"])
    r.table(["Replicas", "In sync now", "Required"], [[len(partition["replicas"]), in_sync, 3]], name)

    async with producer(acks=1) as p:
        await p.send_and_wait(name, value=b"written with acks=1")
    r.step("acks=1 succeeded, because it only needs the leader.")

    error = None
    async with producer(acks="all", request_timeout_ms=3000, retry_backoff_ms=200) as p:
        try:
            await p.send_and_wait(name, value=b"written with acks=all")
        except KafkaError as exc:
            error = type(exc).__name__

    if in_sync >= 3:
        r.step("All 3 copies are in sync, so acks=all was allowed. Stop a broker to see it refused.")
        r.check("acks=all succeeded while all 3 copies are in sync", error is None, error)
    else:
        r.step(f"Only {in_sync} copies are in sync, so Kafka must refuse an acks=all write.")
        r.check("acks=all was refused", error is not None, error)


@feature(
    "batching", GROUP, "Batching",
    "The producer groups messages into batches, so many messages travel in one network trip.",
    "This is a main reason Kafka is fast. The batch counts below come from Kafka's real files on disk.",
    "AIOKafkaProducer(linger_ms=20)   # wait up to 20 ms for company",
)
async def batching(r: Report) -> None:
    count = 500
    value = sample_event(1)
    async with topic("batch-off") as one_by_one, topic("batch-on") as together:
        async with producer(linger_ms=0, acks="all") as p:
            started = time.monotonic()
            for _ in range(count):
                await p.send_and_wait(one_by_one, value=value)
            slow = time.monotonic() - started

        async with producer(linger_ms=20, acks="all") as p:
            started = time.monotonic()
            waiting = [await p.send(together, value=value) for _ in range(count)]
            await asyncio.gather(*waiting)
            fast = time.monotonic() - started

        await kit.wait_until(lambda: sum(b["records"] for b in disk_batches(together)) == count, what="replication")
        await kit.wait_until(lambda: sum(b["records"] for b in disk_batches(one_by_one)) == count, what="replication")
        a, b = disk_batches(one_by_one), disk_batches(together)
        r.table(
            ["Method", "Time", "Messages per second", "Batches on disk"],
            [
                ["Wait for each message", f"{slow:.2f} s", f"{count / slow:,.0f}", len(a)],
                ["Send together, linger 20 ms", f"{fast:.2f} s", f"{count / fast:,.0f}", len(b)],
            ],
            f"{count} identical messages",
        )
        r.check("Batching used fewer batches on disk", len(b) < len(a), f"{len(a)} became {len(b)}")
        r.check("Batching was faster", fast < slow, f"{slow / fast:.1f} times faster")

        async with producer(linger_ms=100) as p:
            started = time.monotonic()
            await p.send_and_wait(one_by_one, value=value)
            waited = (time.monotonic() - started) * 1000
        r.check("linger_ms=100 really made a lone message wait", waited >= 90, f"{waited:.0f} ms")


@feature(
    "compression", GROUP, "Compression: gzip, snappy, lz4, zstd",
    "The producer compresses whole batches. Consumers decompress them automatically.",
    "JSON repeats itself, so it shrinks well. Sizes below are measured from Kafka's real files.",
    'AIOKafkaProducer(compression_type="zstd")',
)
async def compression(r: Report) -> None:
    count = 3000
    values = [sample_event(i) for i in range(count)]
    rows, sizes = [], {}
    for codec in (None, "gzip", "snappy", "lz4", "zstd"):
        label = codec or "none"
        async with topic(f"codec-{label}") as name:
            async with producer(compression_type=codec, linger_ms=20) as p:
                started = time.monotonic()
                waiting = [await p.send(name, value=v) for v in values]
                await asyncio.gather(*waiting)
                elapsed = time.monotonic() - started
            await kit.wait_until(lambda: sum(b["records"] for b in disk_batches(name)) == count, what="replication")
            batches = disk_batches(name)
            size = sum(b["bytes"] for b in batches)
            stored = {b["codec"] for b in batches}
            read_back = await read_topic(name)
            r.check(f"{label}: all {count} messages read back correctly",
                    [m.value for m in read_back] == values and stored == {label}, stored)
            sizes[label] = size
            rows.append([label, f"{size:,} bytes", f"{elapsed:.2f} s"])
    for row in rows:
        row.insert(2, f"{sizes['none'] / sizes[row[0]]:.1f} times smaller" if row[0] != "none" else "-")
    r.table(["Codec", "Size on disk", "Saving", "Send time"], rows, f"{count} ride events")
    r.check("Every codec beat no compression", all(s < sizes["none"] for c, s in sizes.items() if c != "none"))


def lose_first_ack(client) -> dict:
    """The request reaches the broker, but the client never gets the reply.

    This is the exact failure idempotence exists for: the producer cannot know
    the message was stored, so it retries. The broker's reaction below is real.
    """
    real_send = client.send
    state = {"lost": 0}

    async def send(node_id, request, **options):
        response = await real_send(node_id, request, **options)
        if type(request).__name__.startswith("ProduceRequest") and not state["lost"]:
            state["lost"] = 1
            raise RequestTimedOutError("the ack was lost on its way back")
        return response

    client.send = send
    return state


@feature(
    "idempotence", GROUP, "Idempotent producer: a retry cannot duplicate",
    "Each producer numbers its messages. If a retry arrives with a number already stored, the broker drops it.",
    "Without this, a lost acknowledgement makes the producer store the same message twice.",
    "AIOKafkaProducer(enable_idempotence=True)",
)
async def idempotence(r: Report) -> None:
    r.step("In both runs the broker stores the message, then the acknowledgement is lost.")
    async with topic("no-idem") as plain, topic("idem") as safe:
        async with producer(enable_idempotence=False, acks="all", retry_backoff_ms=50) as p:
            state = lose_first_ack(p.client)
            await p.send_and_wait(plain, value=b"TRIP-1")
            r.check("Without idempotence, the ack was lost and the client retried", state["lost"] == 1)

        async with producer(enable_idempotence=True, retry_backoff_ms=50) as p:
            state = lose_first_ack(p.client)
            meta = await p.send_and_wait(safe, value=b"TRIP-1")
            producer_id = p._txn_manager.producer_id
            r.check("With idempotence, the ack was lost and the client retried", state["lost"] == 1)

        a = await read_topic(plain)
        b = await read_topic(safe)
        r.table(
            ["Producer", "Messages stored", "Offsets"],
            [
                ["Idempotence off", len(a), [m.offset for m in a]],
                [f"Idempotence on (producer id {producer_id})", len(b), [m.offset for m in b]],
            ],
        )
        r.check("Idempotence off stored a duplicate", len(a) == 2, len(a))
        r.check("Idempotence on stored it exactly once", len(b) == 1 and meta.offset == 0, len(b))


@feature(
    "quota", GROUP, "Client quotas",
    "A quota caps how fast one client may write. kafka-init limits the client lab-throttled to 256 KB per second.",
    "One noisy client cannot take the whole cluster's capacity from everyone else.",
    "kafka-configs.sh --alter --add-config producer_byte_rate=262144 \\\n"
    "  --entity-type clients --entity-name lab-throttled",
)
async def quota(r: Report) -> None:
    # Kafka averages a client's speed over about 11 seconds, so a short burst
    # stays under the limit. This is big enough to go well past it.
    payload = b"x" * (32 * 1024)
    count = 150
    total_mb = len(payload) * count / (1024 * 1024)

    async def timed(client_id: str, name: str) -> float:
        async with producer(client_id=client_id) as p:
            started = time.monotonic()
            for _ in range(count):
                await p.send_and_wait(name, value=payload)
            return time.monotonic() - started

    async with topic("quota") as name:
        free = await timed("lab-free", name)
        limited = await timed("lab-throttled", name)
        r.table(
            ["Client id", f"Time to send {total_mb:.1f} MB", "Speed"],
            [
                ["lab-free", f"{free:.2f} s", f"{total_mb / free:.1f} MB/s"],
                ["lab-throttled", f"{limited:.2f} s", f"{total_mb / limited:.1f} MB/s"],
            ],
        )
        r.check("The broker slowed the limited client", limited > free * 3, f"{limited / free:.1f} times slower")
        r.check("Nothing was lost, it was only delayed", len(await read_topic(name)) == count * 2)
