"""The Kafka consumer loop."""

import asyncio
import time
from aiokafka import AIOKafkaConsumer
from aiokafka.structs import TopicPartition
from app import config
from app.checkpoint import Checkpoint
from app.state import analytics


class RideConsumer:
    def __init__(self) -> None:
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._run_forever())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    async def _run_forever(self) -> None:
        """Kafka not up yet, or connection dropped? Just try again."""
        while True:
            try:
                await self._consume()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"[{config.SERVICE_NAME}] consumer error: {exc!r} -- retry in 5s")
                await asyncio.sleep(5)

    async def _consume(self) -> None:
        checkpoint = Checkpoint()
        await checkpoint.start()
        consumer = None
        try:
            # Back to the last saved truth. This also throws away anything counted
            # after the last checkpoint, which is about to be read again.
            analytics.restore(await checkpoint.load())

            consumer = AIOKafkaConsumer(
                config.TOPIC,
                bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                group_id=config.GROUP_ID,
                # deliberately NO value_deserializer: a deserializer raises inside
                # getmany(), where a single broken record would take down the whole
                # batch and the service would crash-loop on it forever.
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                max_partition_fetch_bytes=2 * 1024 * 1024,
            )
            await consumer.start()
            analytics.connected = True
            print(f"[{config.SERVICE_NAME}] reading '{config.TOPIC}' as group '{config.GROUP_ID}'")

            offsets: dict[TopicPartition, int] = {}
            last_lag_check = last_saved = 0.0
            while True:
                batches = await consumer.getmany(
                    timeout_ms=config.POLL_TIMEOUT_MS, max_records=config.MAX_RECORDS
                )
                for tp, messages in batches.items():
                    for message in messages:
                        try:
                            analytics.record(message)
                        except Exception as exc:
                            analytics.bad += 1
                            print(
                                f"[{config.SERVICE_NAME}] skipping bad message at "
                                f"{message.partition}@{message.offset} ({exc!r})"
                            )
                        if analytics.throttle_ms:
                            await asyncio.sleep(analytics.throttle_ms / 1000)
                    offsets[tp] = messages[-1].offset + 1

                now = time.monotonic()
                due = (offsets and now - last_saved >= config.CHECKPOINT_SEC) or analytics.needs_saving
                if due:
                    await checkpoint.save(analytics.dump(), offsets)
                    offsets = {}
                    last_saved = now
                    analytics.needs_saving = False

                if now - last_lag_check > config.LAG_REFRESH_SEC:
                    last_lag_check = now
                    await self._update_lag(consumer)
        finally:
            analytics.connected = False
            if consumer:
                await consumer.stop()
            await checkpoint.stop()
            print(f"[{config.SERVICE_NAME}] consumer stopped")

    @staticmethod
    async def _update_lag(consumer: AIOKafkaConsumer) -> None:
        """Lag = messages that exist in the partition but we have not processed yet.

        Measured against the COMMITTED offset, not the fetch position: the fetch
        position runs ahead into the client's buffer, so it would report 0 lag
        while the work is still queued up. Committed offset is also exactly what
        `kafka-consumer-groups.sh --describe` prints in its LAG column.
        """
        try:
            assigned = list(consumer.assignment())
            if not assigned:
                return
            end_offsets = await consumer.end_offsets(assigned)
            total = 0
            for tp in assigned:
                committed = await consumer.committed(tp)
                if committed is None:
                    committed = await consumer.position(tp)
                lag = max(0, end_offsets[tp] - committed)
                analytics.set_lag(tp.partition, lag)
                total += lag
            analytics.lag = total
        except Exception:
            pass


ride_consumer = RideConsumer()
