"""Brokers, replication, the KRaft controller and surviving a broker failure."""

import asyncio
import time
from collections import Counter
from aiokafka.errors import KafkaError
from app import kit
from app.kit import Report, admin, producer, read_topic, topic
from app.registry import feature

GROUP = "The cluster"


@feature(
    "brokers", GROUP, "Brokers and the KRaft controller",
    "The cluster is three brokers. One is the active controller, which keeps the cluster's records.",
    "Kafka runs with no ZooKeeper. The controllers elect a leader between themselves using KRaft.",
    "await admin.describe_cluster()",
)
async def brokers(r: Report) -> None:
    async with admin() as a:
        info = await a.describe_cluster()
    r.table(
        ["Broker id", "Address", "Role"],
        [
            [b["node_id"], f'{b["host"]}:{b["port"]}',
             "broker + active controller" if b["node_id"] == info["controller_id"] else "broker + standby controller"]
            for b in sorted(info["brokers"], key=lambda b: b["node_id"])
        ],
        f'Cluster id {info["cluster_id"]}',
    )
    r.check("All 3 brokers are alive", len(info["brokers"]) == 3, len(info["brokers"]))
    r.check("One broker is the active controller", info["controller_id"] in {b["node_id"] for b in info["brokers"]}, info["controller_id"])


@feature(
    "replication", GROUP, "Replication: leaders, followers and ISR",
    "Each partition has 3 copies on 3 brokers. One leader serves clients, and followers keep copying it.",
    "ISR means in-sync replicas, the copies that are fully caught up and can take over at any time.",
    "await admin.describe_topics([\"ride-events\"])",
)
async def replication(r: Report) -> None:
    async with admin() as a:
        info = (await a.describe_topics(["ride-events"]))[0]
    partitions = info["partitions"]
    r.table(
        ["Partition", "Leader", "Replicas", "In sync (ISR)"],
        [[f'P{p["partition"]}', p["leader"], p["replicas"], p["isr"]] for p in sorted(partitions, key=lambda p: p["partition"])],
        "The real layout of ride-events",
    )
    leaders = Counter(p["leader"] for p in partitions)
    r.check("Every partition has 3 replicas", all(len(p["replicas"]) == 3 for p in partitions))
    r.check("Every replica is in sync", all(len(p["isr"]) == 3 for p in partitions))
    r.check("Leaders are spread across the brokers", len(leaders) == 3, dict(leaders))


@feature(
    "failover", GROUP, "Survive a broker failure",
    "This writes for 15 seconds with acks=all, then checks that nothing was lost or duplicated.",
    "Stop a broker while it runs and watch leaders move on the cluster panel. The test must still pass.",
    "docker compose stop kafka-2     # while this runs\ndocker compose start kafka-2    # afterwards",
)
async def failover(r: Report) -> None:
    seconds = float(r.options.get("seconds", 15))
    async with topic("failover", partitions=3) as name:
        async with admin() as a:
            before = (await a.describe_topics([name]))[0]["partitions"]
        sent, failed = [], 0
        leaders_seen = {p["partition"]: {p["leader"]} for p in before}

        async with producer(acks="all", enable_idempotence=True, linger_ms=5, request_timeout_ms=30000) as p:
            ended = time.monotonic() + seconds
            next_sample = 0.0
            while time.monotonic() < ended:
                value = kit.dumps({"n": len(sent)})
                sent.append(await p.send(name, key=str(len(sent)).encode(), value=value))
                await asyncio.sleep(0.01)
                if time.monotonic() > next_sample:
                    next_sample = time.monotonic() + 1
                    try:
                        async with admin() as a:
                            for item in (await a.describe_topics([name]))[0]["partitions"]:
                                leaders_seen[item["partition"]].add(item["leader"])
                    except KafkaError:
                        pass   # the admin connection may have used the stopped broker
            results = await asyncio.gather(*sent, return_exceptions=True)

        failed = sum(isinstance(item, Exception) for item in results)
        acknowledged = {i for i, item in enumerate(results) if not isinstance(item, Exception)}
        stored = await read_topic(name, partitions=3)
        numbers = [kit.loads(m.value)["n"] for m in stored]
        moved = {p: sorted(v) for p, v in leaders_seen.items() if len(v - {-1}) > 1}

        r.table(
            ["Measure", "Value"],
            [
                ["Messages sent", len(sent)],
                ["Acknowledged by Kafka", len(acknowledged)],
                ["Read back", len(numbers)],
                ["Partitions whose leader moved", moved or "none, no broker was stopped"],
            ],
        )
        r.check("Every acknowledged message survived", acknowledged <= set(numbers), f"{len(acknowledged - set(numbers))} lost")
        r.check("No message was stored twice", len(numbers) == len(set(numbers)), f"{len(numbers) - len(set(numbers))} duplicates")
        r.check("No send failed", failed == 0, failed)
