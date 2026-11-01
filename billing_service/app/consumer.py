"""
The read-process-write pattern, with exactly-once processing.

This service is a consumer AND a producer at the same time:

    ride-events  ──read──►  billing-service  ──write──►  invoices

It reads the same topic as the analytics service, but under its own group id,
so both services receive every single message independently.

The invoices, the totals and the ride-events offsets all commit in one Kafka
transaction. If anything fails, all of it is cancelled and the same rides are
processed again, so each completed trip produces exactly one visible invoice.
Anyone reading the invoices topic must use isolation_level="read_committed".
"""

import asyncio
import json
import time
from datetime import datetime, timezone
from aiokafka import AIOKafkaConsumer
from aiokafka.structs import TopicPartition
from app import config
from app.producer import invoice_producer
from app.state import billing


def build_invoice(event: dict, msg) -> dict:
    """Turn a completed trip into an invoice."""
    fare = event["fare"]
    commission = round(fare * config.COMMISSION_RATE, 2)
    return {
        "invoice_id": f"INV-{msg.partition}-{msg.offset}",
        "trip_id": event["trip_id"],
        "driver_id": event["driver_id"],
        "rider_id": event["rider_id"],
        "zone": event["zone"],
        "vehicle": event["vehicle"],
        "distance_km": event["distance_km"],
        "fare": fare,
        "commission": commission,
        "driver_payout": round(fare - commission, 2),
        "source": {"topic": msg.topic, "partition": msg.partition, "offset": msg.offset},
        "issued_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
    }


async def load_saved_state() -> dict | None:
    """Read the newest totals committed for this group, if there are any."""
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
                    if message.key == config.GROUP_ID.encode():
                        saved = json.loads(message.value) if message.value else None
        return saved
    finally:
        await reader.stop()


class BillingConsumer:
    def __init__(self) -> None:
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._run_forever())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    async def _run_forever(self) -> None:
        while True:
            try:
                await self._consume()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"[{config.SERVICE_NAME}] consumer error: {exc!r} -- retry in 5s")
                await asyncio.sleep(5)

    async def _consume(self) -> None:
        # Starting the producer first cancels anything a crashed instance left open.
        await invoice_producer.start()
        consumer = None
        in_transaction = False
        try:
            # Back to the last committed truth. Totals counted after that belong
            # to a transaction that was cancelled, and will be read again.
            billing.restore(await load_saved_state())

            consumer = AIOKafkaConsumer(
                config.SOURCE_TOPIC,
                bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                group_id=config.GROUP_ID,
                # deliberately NO value_deserializer: a deserializer raises inside
                # getmany(), where there is no way to isolate the bad record.
                # We parse each message ourselves and route failures to the DLQ.
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                max_partition_fetch_bytes=2 * 1024 * 1024,
            )
            await consumer.start()
            billing.connected = True
            print(f"[{config.SERVICE_NAME}] reading '{config.SOURCE_TOPIC}' as group '{config.GROUP_ID}'")

            offsets: dict[TopicPartition, int] = {}
            last_lag_check = opened_at = 0.0
            while True:
                batches = await consumer.getmany(
                    timeout_ms=config.POLL_TIMEOUT_MS, max_records=config.MAX_RECORDS
                )
                if (batches or billing.needs_saving) and not in_transaction:
                    await invoice_producer.begin()
                    in_transaction = True
                    opened_at = time.monotonic()

                for tp, messages in batches.items():
                    for msg in messages:
                        billing.touch_partition(msg.partition, msg.offset)
                        try:
                            event = json.loads(msg.value)
                            if event["event_type"] != "completed":
                                billing.skip()
                                continue
                            invoice = build_invoice(event, msg)
                        except Exception as exc:
                            print(
                                f"[{config.SERVICE_NAME}] bad message at "
                                f"{msg.partition}@{msg.offset} -> DLQ ({exc!r})"
                            )
                            await invoice_producer.send_dead_letter(msg, exc)
                            billing.poison()
                            continue

                        await invoice_producer.send(invoice)
                        billing.record(invoice)
                    offsets[tp] = messages[-1].offset + 1

                now = time.monotonic()
                if in_transaction and (now - opened_at >= config.CHECKPOINT_SEC or billing.needs_saving):
                    await invoice_producer.commit(billing.dump(), offsets)
                    in_transaction = False
                    offsets = {}
                    billing.needs_saving = False

                if now - last_lag_check > config.LAG_REFRESH_SEC:
                    last_lag_check = now
                    await self._update_lag(consumer)
        finally:
            billing.connected = False
            if consumer:
                await consumer.stop()
            if in_transaction:
                try:
                    await invoice_producer.abort()
                except Exception as exc:
                    print(f"[{config.SERVICE_NAME}] could not abort: {exc!r}")
            await invoice_producer.stop()

    @staticmethod
    async def _update_lag(consumer: AIOKafkaConsumer) -> None:
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
                billing.set_lag(tp.partition, lag)
                total += lag
            billing.lag = total
        except Exception:
            pass


billing_consumer = BillingConsumer()
