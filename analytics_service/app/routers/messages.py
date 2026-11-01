"""
Read any single message back out of Kafka, by partition + offset.

This is the endpoint behind "click a row in the live feed". It proves the point
that a queue cannot: the message was consumed minutes ago, and it is *still
there*, byte for byte, because reading in Kafka does not delete anything.

A brand new consumer is used with NO group_id -- it joins no group, commits
nothing, and disturbs no other consumer. It just seeks to the offset and reads.
"""

import asyncio
import json
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError
from aiokafka.structs import TopicPartition
from fastapi import APIRouter, HTTPException
from app import config

router = APIRouter(tags=["messages"])


@router.get("/messages/{partition}/{offset}")
async def read_message(partition: int, offset: int):
    consumer = AIOKafkaConsumer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        enable_auto_commit=False,
        group_id=None,
    )
    await consumer.start()
    try:
        tp = TopicPartition(config.TOPIC, partition)
        consumer.assign([tp])

        try:
            first = (await asyncio.wait_for(consumer.beginning_offsets([tp]), 5))[tp]
            last = (await asyncio.wait_for(consumer.end_offsets([tp]), 5))[tp]
        except (asyncio.TimeoutError, KafkaError):
            raise HTTPException(404, f"topic {config.TOPIC} has no partition {partition}")
        if not first <= offset < last:
            raise HTTPException(
                404,
                f"partition {partition} currently holds offsets {first}..{last - 1}",
            )

        consumer.seek(tp, offset)
        msg = await asyncio.wait_for(consumer.getone(), timeout=5.0)

        raw = msg.value.decode("utf-8", "replace") if msg.value is not None else None
        try:
            value = json.loads(raw)
        except (TypeError, ValueError):
            value = None

        leader = consumer._client.cluster.leader_for_partition(tp)
        broker = consumer._client.cluster.broker_metadata(leader)
        return {
            "leader": leader,
            "leader_address": f"{broker.host}:{broker.port}" if broker else None,
            "topic": msg.topic,
            "partition": msg.partition,
            "offset": msg.offset,
            "key": msg.key.decode("utf-8") if msg.key else None,
            "timestamp_ms": msg.timestamp,
            "key_bytes": msg.serialized_key_size,
            "value_bytes": msg.serialized_value_size,
            "headers": [[k, v.decode("utf-8", "replace")] for k, v in (msg.headers or [])],
            "partition_range": {"first": first, "last": last - 1},
            "raw": raw,
            "value": value,
        }
    except asyncio.TimeoutError:
        raise HTTPException(504, "Kafka did not return the message in time")
    finally:
        await consumer.stop()
