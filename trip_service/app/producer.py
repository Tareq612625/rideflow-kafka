"""Everything that touches Kafka lives here. The routers never import aiokafka."""

import asyncio
import json
import time
from collections import defaultdict
from aiokafka import AIOKafkaProducer
from app import config
from app.events import trip_lifecycle


def serialize(value) -> bytes:
    """JSON for events. Raw bytes pass straight through, so a broken message can be sent on purpose."""
    return value if isinstance(value, bytes) else json.dumps(value).encode("utf-8")


def headers_for(event: dict) -> list[tuple[str, bytes]]:
    """Labels stored beside the message, so a reader need not parse the value."""
    return [
        ("trace-id", event["event_id"].encode()),
        ("schema-version", b"2"),
        ("source", config.SERVICE_NAME.encode()),
    ]


class RideProducer:
    """Thin wrapper around AIOKafkaProducer + the burst generator."""

    def __init__(self) -> None:
        self._producer: AIOKafkaProducer | None = None
        self._burst: asyncio.Task | None = None
        self.sent = 0
        self.per_partition: dict[int, int] = defaultdict(int)
        self.job = {
            "running": False, "total": 0, "done": 0, "failed": 0,
            "rate": 0, "started": 0.0, "elapsed": 0.0,
        }

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=serialize,
            key_serializer=lambda k: k.encode("utf-8"),
            acks="all",
            enable_idempotence=True,
            linger_ms=config.LINGER_MS,
            max_batch_size=config.MAX_BATCH_SIZE,
            compression_type=config.COMPRESSION,
        )
        await self._producer.start()
        print(f"[{config.SERVICE_NAME}] connected to Kafka at {config.KAFKA_BOOTSTRAP_SERVERS}")

    async def stop(self) -> None:
        if self._burst and not self._burst.done():
            self._burst.cancel()
        if self._producer:
            await self._producer.stop()

    async def send_one(self, event: dict):
        """Publish a single event and wait for Kafka to acknowledge it.

        The KEY is driver_id: Kafka hashes it to choose the partition, so every
        event of one driver lands in the same partition and stays in order.
        """
        metadata = await self._producer.send_and_wait(
            config.TOPIC, key=event["driver_id"], value=event, headers=headers_for(event)
        )
        self.per_partition[metadata.partition] += 1
        self.sent += 1
        return metadata

    async def send_broken(self, kind: str):
        """Publish a message no consumer can use, to watch the error handling work.

        Billing parks it in the dead letter topic. Analytics counts it and skips it.
        """
        value = b"{this is not json" if kind == "not-json" else {"trip_id": "TRIP-BROKEN"}
        return await self._producer.send_and_wait(config.TOPIC, key="DRV-BROKEN", value=value)

    async def _settle(self, pending: list) -> None:
        """Wait for a chunk of in-flight sends and record where they landed."""
        if not pending:
            return
        for result in await asyncio.gather(*pending, return_exceptions=True):
            if isinstance(result, BaseException):
                self.job["failed"] += 1
                continue
            self.per_partition[result.partition] += 1
            self.sent += 1
            self.job["done"] += 1
        pending.clear()

    async def _run_burst(self, count: int, rate: int, drivers: int) -> None:
        self.job.update(
            running=True, total=count, done=0, failed=0, rate=rate, started=time.time(), elapsed=0.0
        )
        pending: list = []
        tick = max(1, rate // 20) if rate else 0  # when pacing, sleep 20x per second
        queued = 0
        try:
            while queued < count:
                for event in trip_lifecycle(drivers)[: count - queued]:
                    pending.append(
                        await self._producer.send(
                            config.TOPIC, key=event["driver_id"], value=event, headers=headers_for(event)
                        )
                    )
                    queued += 1
                    if len(pending) >= 500:
                        await self._settle(pending)
                    if tick and queued % tick == 0:
                        await asyncio.sleep(0.05)
            await self._settle(pending)
        except asyncio.CancelledError:
            await self._settle(pending)
            raise
        finally:
            self.job["elapsed"] = round(time.time() - self.job["started"], 3)
            self.job["running"] = False

    def start_burst(self, count: int, rate: int, drivers: int) -> bool:
        if self.job["running"]:
            return False
        self._burst = asyncio.create_task(self._run_burst(count, rate, drivers))
        return True

    def stop_burst(self) -> bool:
        if self._burst and not self._burst.done():
            self._burst.cancel()
            return True
        return False

    def stats(self) -> dict:
        job = dict(self.job)
        if job["running"]:
            job["elapsed"] = round(time.time() - job["started"], 3)
        return {
            "produced": self.sent,
            "per_partition": {str(k): v for k, v in sorted(self.per_partition.items())},
            "job": job,
        }

    def reset(self) -> None:
        self.sent = 0
        self.per_partition.clear()


producer = RideProducer()
