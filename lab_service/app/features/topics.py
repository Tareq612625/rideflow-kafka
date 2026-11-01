"""Topics, partitions, keys, headers and timestamps."""

import time
from collections import defaultdict
from aiokafka.admin import NewPartitions, NewTopic
from aiokafka.admin.config_resource import ConfigResource, ConfigResourceType
from aiokafka.errors import KafkaError
from aiokafka.partitioner import murmur2
from app import kit
from app.kit import Report, admin, consumer, producer, read_topic, topic
from app.registry import feature

GROUP = "Topics and messages"


async def topic_config(client, name: str) -> dict[str, str]:
    response = await client.describe_configs([ConfigResource(ConfigResourceType.TOPIC, name)])
    entries = response[0].to_object()["resources"][0]["config_entries"]
    return {e["config_names"]: e["config_value"] for e in entries}


@feature(
    "topic-admin", GROUP, "Create, change and delete a topic",
    "The Admin API manages topics from code: create one, add partitions, change a setting, delete it.",
    "Real systems create topics on purpose, with the right partitions and settings, rather than by accident.",
    'await admin.create_topics([NewTopic("orders", 3, 3)])\n'
    'await admin.create_partitions({"orders": NewPartitions(6)})',
)
async def topic_admin(r: Report) -> None:
    name = kit.unique("admin")
    async with admin() as client:
        await client.create_topics(
            [NewTopic(name, 3, 3, topic_configs={"retention.ms": "3600000"})]
        )
        await kit.wait_until(lambda: kit._topic_ready(client, name, 3), what="the new topic")
        r.step(f"Created {name} with 3 partitions, replication factor 3, retention 1 hour.")

        await client.create_partitions({name: NewPartitions(6)})
        await kit.wait_until(lambda: kit._topic_ready(client, name, 6), what="the new partitions")
        info = (await client.describe_topics([name]))[0]
        r.check("Partitions grew from 3 to 6", len(info["partitions"]) == 6, len(info["partitions"]))

        await client.alter_configs(
            [ConfigResource(ConfigResourceType.TOPIC, name, configs={"retention.ms": "7200000"})]
        )
        async def changed():
            return (await topic_config(client, name)).get("retention.ms") == "7200000"
        r.check("Retention changed to 2 hours", await kit.wait_until(changed, what="the new setting"))

        await client.delete_topics([name])
        async def gone():
            return name not in await client.list_topics()
        r.check("Topic deleted", await kit.wait_until(gone, what="the topic to disappear"))


@feature(
    "offsets", GROUP, "Partitions and offsets",
    "A topic is split into partitions. Each partition is its own log, and its offsets count up from 0.",
    "An exact message is found by three things: topic, partition and offset.",
    "meta = await producer.send_and_wait(topic, key=key, value=value)\n"
    "print(meta.partition, meta.offset)",
)
async def offsets(r: Report) -> None:
    async with topic("offsets", partitions=6) as name, producer(acks="all") as p:
        seen = defaultdict(list)
        for i in range(60):
            meta = await p.send_and_wait(name, key=f"DRV-{i:04d}".encode(), value=b"ride")
            seen[meta.partition].append(meta.offset)
        r.table(
            ["Partition", "Messages", "Offsets"],
            [[f"P{p}", len(o), f"{o[0]} to {o[-1]}"] for p, o in sorted(seen.items())],
            "Where 60 messages really landed",
        )
        r.check("Messages spread across several partitions", len(seen) >= 3, f"{len(seen)} partitions used")
        r.check(
            "Offsets in every partition count 0, 1, 2 with no gaps",
            all(o == list(range(len(o))) for o in seen.values()),
        )


@feature(
    "keys", GROUP, "Same key, same partition, same order",
    "The key is hashed to choose a partition, so all messages with one key stay together and in order.",
    "This is how Kafka guarantees that one driver's events can never overtake each other.",
    "partition = murmur2(key) % number_of_partitions",
)
async def keys(r: Report) -> None:
    drivers = ["DRV-0184", "DRV-0007", "DRV-0042", "DRV-0100", "DRV-0250"]
    async with topic("keys", partitions=6) as name, producer(acks="all") as p:
        placed = defaultdict(set)
        for sequence in range(20):
            for driver in drivers:
                meta = await p.send_and_wait(
                    name, key=driver.encode(), value=kit.dumps({"seq": sequence})
                )
                placed[driver].add(meta.partition)

        received = defaultdict(list)
        for message in await read_topic(name, partitions=6):
            received[message.key.decode()].append(kit.loads(message.value)["seq"])

        rows = []
        for driver in drivers:
            hashed = murmur2(driver.encode()) & 0x7FFFFFFF
            rows.append([driver, hashed, f"{hashed} % 6 = {hashed % 6}", f"P{min(placed[driver])}"])
        r.table(["Key", "murmur2 hash", "Calculation", "Kafka stored it in"], rows)
        r.check("Each key used exactly one partition", all(len(s) == 1 for s in placed.values()))
        r.check(
            "The hash calculation matches where Kafka stored it",
            all((murmur2(d.encode()) & 0x7FFFFFFF) % 6 == min(placed[d]) for d in drivers),
        )
        r.check(
            "Every key read back in the exact order it was sent",
            all(received[d] == list(range(20)) for d in drivers),
        )


@feature(
    "no-key", GROUP, "Messages with no key",
    "With no key, the producer spreads messages across partitions to balance the load.",
    "Use this when order does not matter. You gain an even spread and lose per-key ordering.",
    "await producer.send(topic, value=value)   # no key",
)
async def no_key(r: Report) -> None:
    async with topic("no-key", partitions=6) as name, producer(acks="all") as p:
        counts = defaultdict(int)
        for _ in range(300):
            meta = await p.send_and_wait(name, value=b"anything")
            counts[meta.partition] += 1
        r.table(["Partition", "Messages"], [[f"P{p}", n] for p, n in sorted(counts.items())])
        r.check("All 6 partitions received messages", len(counts) == 6, f"{len(counts)} used")


@feature(
    "custom-partitioner", GROUP, "A custom partitioner",
    "You can replace the hash with your own rule. Here VIP drivers always go to partition 0.",
    "Useful when one class of messages needs its own lane, so that it can be read first.",
    "def vip_first(key, all_partitions, available):\n"
    '    if key.startswith(b"VIP-"):\n'
    "        return 0\n"
    "    return 1 + murmur2(key) % (len(all_partitions) - 1)",
)
async def custom_partitioner(r: Report) -> None:
    def vip_first(key, all_partitions, available):
        if key.startswith(b"VIP-"):
            return 0
        return 1 + (murmur2(key) & 0x7FFFFFFF) % (len(all_partitions) - 1)

    async with topic("partitioner", partitions=4) as name, producer(partitioner=vip_first) as p:
        vip, normal = set(), set()
        for i in range(40):
            vip.add((await p.send_and_wait(name, key=f"VIP-{i}".encode(), value=b"x")).partition)
            normal.add((await p.send_and_wait(name, key=f"DRV-{i}".encode(), value=b"x")).partition)
        r.table(["Keys", "Partitions used"], [["VIP-*", sorted(vip)], ["DRV-*", sorted(normal)]])
        r.check("Every VIP message went to partition 0", vip == {0}, sorted(vip))
        r.check("No normal message went to partition 0", 0 not in normal, sorted(normal))


@feature(
    "headers", GROUP, "Message headers",
    "Headers are small labels stored beside the message, outside the value.",
    "They carry things like a trace id or a schema version, so readers need not parse the value.",
    'headers=[("trace-id", b"a1b2"), ("schema-version", b"2")]',
)
async def headers(r: Report) -> None:
    sent = [("trace-id", b"a1b2c3"), ("schema-version", b"2"), ("source", b"lab")]
    async with topic("headers") as name, producer() as p:
        await p.send_and_wait(name, key=b"k", value=b"{}", headers=sent)
        message = (await read_topic(name))[0]
        got = [(k, v) for k, v in message.headers]
        r.table(["Header", "Value read back"], [[k, v.decode()] for k, v in got])
        r.check("All headers came back unchanged", got == sent)


@feature(
    "timestamps", GROUP, "CreateTime and LogAppendTime",
    "A message's timestamp comes from the producer by default. A topic can tell the broker to set it instead.",
    "Use broker time when you cannot trust the clocks on the machines sending data.",
    '{"message.timestamp.type": "LogAppendTime"}',
)
async def timestamps(r: Report) -> None:
    one_hour_ago = int((time.time() - 3600) * 1000)
    async with topic("create-time") as create, producer() as p:
        async with topic("append-time", configs={"message.timestamp.type": "LogAppendTime"}) as append:
            await p.send_and_wait(create, value=b"x", timestamp_ms=one_hour_ago)
            await p.send_and_wait(append, value=b"x", timestamp_ms=one_hour_ago)
            a = (await read_topic(create))[0]
            b = (await read_topic(append))[0]
            r.step("Sent one message to each topic, claiming it was created one hour ago.")
            r.table(
                ["Topic setting", "Timestamp type", "Age of stored timestamp"],
                [
                    ["CreateTime (default)", a.timestamp_type, f"{(time.time() * 1000 - a.timestamp) / 1000:.0f} s"],
                    ["LogAppendTime", b.timestamp_type, f"{(time.time() * 1000 - b.timestamp) / 1000:.0f} s"],
                ],
            )
            r.check("CreateTime kept the producer's value", a.timestamp == one_hour_ago)
            r.check("LogAppendTime replaced it with broker time", abs(time.time() * 1000 - b.timestamp) < 60_000)


@feature(
    "message-size", GROUP, "The maximum message size",
    "Each topic has a size limit per message. A bigger message is refused by the broker.",
    "Kafka works best with small messages. Store a large file elsewhere and send a link to it.",
    '{"max.message.bytes": "2048"}',
)
async def message_size(r: Report) -> None:
    async with topic("max-size", configs={"max.message.bytes": "2048"}) as name, producer() as p:
        await p.send_and_wait(name, value=b"x" * 1000)
        r.step("A 1 KB message was accepted.")
        error = None
        try:
            await p.send_and_wait(name, value=b"x" * 5000)
        except KafkaError as exc:
            error = type(exc).__name__
        r.check("The broker refused a 5 KB message", error == "MessageSizeTooLargeError", error)
        r.check("Only the small message was stored", len(await read_topic(name)) == 1)
