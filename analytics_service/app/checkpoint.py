"""Saves the statistics and the read position together, in one Kafka transaction.

Offsets alone are not enough. If the numbers live only in memory, a restart loses
them while the committed offset still says those messages were handled.

So both go into one transaction: the statistics to a compacted topic, and the
offsets to the consumer group. Either both happen or neither does, so after any
crash the statistics match the position exactly, with nothing lost or counted
twice. Kafka Streams keeps its state safe the same way.
"""

import json
import time
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition
from app import config


class Checkpoint:
    def __init__(self) -> None:
        self._producer: AIOKafkaProducer | None = None
        self._key = config.GROUP_ID.encode()

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            # Stable across restarts, so a new instance cancels whatever the old one
            # left unfinished, and a stalled old instance is refused ("fenced").
            transactional_id=f"{config.GROUP_ID}-state",
            compression_type="gzip",
        )
        await self._producer.start()

    async def stop(self) -> None:
        if self._producer:
            await self._producer.stop()

    async def load(self) -> dict | None:
        """Read the newest saved statistics for this group, if there are any."""
        tp = TopicPartition(config.STATE_TOPIC, 0)
        reader = AIOKafkaConsumer(
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            isolation_level="read_committed",   # never see a cancelled snapshot
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        await reader.start()
        try:
            reader.assign([tp])
            end = (await reader.end_offsets([tp]))[tp]
            await reader.seek_to_beginning(tp)
            saved = None
            deadline = time.monotonic() + 30
            while await reader.position(tp) < end:
                if time.monotonic() > deadline:
                    raise TimeoutError("could not read the saved state in time")
                for messages in (await reader.getmany(timeout_ms=500)).values():
                    for message in messages:
                        if message.key == self._key:
                            saved = json.loads(message.value) if message.value else None
            return saved
        finally:
            await reader.stop()

    async def save(self, state: dict, offsets: dict[TopicPartition, int]) -> None:
        async with self._producer.transaction():
            await self._producer.send(config.STATE_TOPIC, key=self._key, value=json.dumps(state).encode())
            if offsets:
                await self._producer.send_offsets_to_transaction(offsets, config.GROUP_ID)
