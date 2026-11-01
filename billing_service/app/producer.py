"""The writing half of this service: invoices, dead letters and saved state.

Everything here happens inside a Kafka transaction, together with the offsets of
the ride events that caused it. See consumer.py for why.
"""

import json
from datetime import datetime, timezone
from aiokafka import AIOKafkaProducer
from aiokafka.structs import TopicPartition
from app import config


class InvoiceProducer:
    def __init__(self) -> None:
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8"),
            # Stable across restarts, so a new instance cancels whatever the old one
            # left unfinished, and a stalled old instance is refused ("fenced").
            transactional_id=f"{config.GROUP_ID}-invoices",
            linger_ms=10,
            compression_type="gzip",
        )
        await self._producer.start()
        print(f"[{config.SERVICE_NAME}] will publish invoices to '{config.INVOICE_TOPIC}'")

    async def stop(self) -> None:
        if self._producer:
            await self._producer.stop()

    async def begin(self) -> None:
        await self._producer.begin_transaction()

    async def abort(self) -> None:
        await self._producer.abort_transaction()

    async def commit(self, state: dict, offsets: dict[TopicPartition, int]) -> None:
        """The moment everything becomes real, all at once: the invoices sent since
        begin(), the service's totals, and the position in ride-events."""
        await self._producer.send(config.STATE_TOPIC, key=config.GROUP_ID, value=state)
        if offsets:
            await self._producer.send_offsets_to_transaction(offsets, config.GROUP_ID)
        await self._producer.commit_transaction()

    async def send(self, invoice: dict):
        """Adds the invoice to the open transaction."""
        return await self._producer.send(
            config.INVOICE_TOPIC,
            key=invoice["driver_id"],   # one driver's invoices stay in order
            value=invoice,
        )

    async def send_dead_letter(self, msg, error: Exception):
        """Park a message this service cannot process.

        Without this, one malformed record stops the whole partition: the
        service crashes, restarts, reads the same record, crashes again —
        forever. The original bytes are kept so nothing is lost.
        """
        return await self._producer.send(
            config.DLQ_TOPIC,
            key=(msg.key.decode("utf-8", "replace") if msg.key else "unknown"),
            value={
                "error": f"{type(error).__name__}: {error}",
                "service": config.SERVICE_NAME,
                "source": {"topic": msg.topic, "partition": msg.partition, "offset": msg.offset},
                "raw": msg.value.decode("utf-8", "replace") if msg.value else None,
                "failed_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            },
        )


invoice_producer = InvoiceProducer()
