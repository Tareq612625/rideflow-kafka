"""Atomic writes, read_committed, exactly-once processing and zombie fencing."""

from aiokafka.errors import KafkaError
from aiokafka.structs import TopicPartition
from app import kit
from app.kit import Report, admin, consumer, producer, read, read_topic, topic
from app.registry import feature

GROUP = "Transactions and exactly-once"


@feature(
    "txn-commit-abort", GROUP, "Commit and abort",
    "A transaction makes several writes succeed or fail as one. An aborted write is hidden from careful readers.",
    "Two topics can be updated together, with no half-finished state ever visible.",
    "async with producer.transaction():\n    await producer.send(topic, value)",
)
async def txn_commit_abort(r: Report) -> None:
    async with topic("txn") as name, producer(transactional_id=kit.unique("tx")) as p:
        await p.begin_transaction()
        await p.send_and_wait(name, value=b"committed")
        await p.commit_transaction()

        await p.begin_transaction()
        await p.send_and_wait(name, value=b"aborted")
        await p.abort_transaction()

        uncommitted = [m.value.decode() for m in await read_topic(name)]
        committed = [m.value.decode() for m in await read_topic(name, isolation_level="read_committed")]
        r.table(
            ["Reader setting", "Sees"],
            [["read_uncommitted (default)", uncommitted], ["read_committed", committed]],
        )
        r.check("read_committed hid the aborted message", committed == ["committed"], committed)
        r.check("The aborted message is still physically in the log", "aborted" in uncommitted, uncommitted)


@feature(
    "isolation", GROUP, "read_committed waits for open transactions",
    "A read_committed consumer stops at the last stable offset, the first message of any open transaction.",
    "Careful readers never see work that might still be cancelled.",
    'AIOKafkaConsumer(topic, isolation_level="read_committed")',
)
async def isolation(r: Report) -> None:
    async with topic("lso") as name, producer(transactional_id=kit.unique("tx")) as p:
        tp = TopicPartition(name, 0)
        await p.begin_transaction()
        await p.send_and_wait(name, value=b"not decided yet")
        async with consumer(isolation_level="read_committed") as careful, consumer() as eager:
            careful.assign([tp])
            eager.assign([tp])
            await careful.seek_to_beginning(tp)
            await eager.seek_to_beginning(tp)
            hidden = await read(careful, quiet_ms=1500)
            visible = await read(eager, quiet_ms=1500)
            stable = (await careful.end_offsets([tp]))[tp]
            high = (await eager.end_offsets([tp]))[tp]
            r.table(
                ["While the transaction is open", "Value"],
                [["High watermark", high], ["Last stable offset", stable],
                 ["read_uncommitted sees", len(visible)], ["read_committed sees", len(hidden)]],
            )
            r.check("The careful reader saw nothing yet", len(hidden) == 0)
            r.check("The default reader saw it early", len(visible) == 1)

            await p.commit_transaction()
            r.step("Committed the transaction.")
            released = await read(careful, want=1)
            r.check("After commit, the careful reader received it", len(released) == 1)


@feature(
    "exactly-once", GROUP, "Exactly-once read-process-write",
    "The output and the input offset are committed in one transaction. A crash cancels both, so the retry is clean.",
    "This removes the duplicates that at-least-once allows. It is what RideFlow billing now does.",
    "async with producer.transaction():\n"
    "    await producer.send(output, result)\n"
    "    await producer.send_offsets_to_transaction({tp: offset}, group_id)",
)
async def exactly_once(r: Report) -> None:
    count = 20
    async with topic("eos-in") as source, topic("eos-out") as output:
        async with producer(acks="all") as p:
            for i in range(count):
                await p.send_and_wait(source, value=kit.dumps({"n": i}))

        group = kit.unique("group")
        transaction_id = kit.unique("tx")
        tp = TopicPartition(source, 0)

        async def attempt(crash: bool) -> None:
            options = dict(group_id=group, isolation_level="read_committed")
            async with consumer(source, **options) as c, producer(transactional_id=transaction_id) as p:
                batch = await read(c, want=count)
                await p.begin_transaction()
                for message in batch:
                    await p.send(output, value=message.value)
                await p.send_offsets_to_transaction({tp: batch[-1].offset + 1}, group)
                if crash:
                    await p.abort_transaction()   # a crash here ends the same way
                else:
                    await p.commit_transaction()

        await attempt(crash=True)
        r.step(f"First attempt processed all {count} messages, then crashed before commit.")
        await attempt(crash=False)
        r.step("Second attempt started again from the same offset, and committed.")

        raw = await read_topic(output)
        clean = await read_topic(output, isolation_level="read_committed")
        async with consumer(group_id=group) as c:
            c.assign([tp])
            committed = await c.committed(tp)
        r.table(
            ["Measure", "Value"],
            [["Messages physically in the output", len(raw)],
             ["Messages a read_committed reader sees", len(clean)],
             ["Group's committed input offset", committed]],
        )
        r.check("The crashed attempt really wrote to the log", len(raw) == count * 2, len(raw))
        r.check("Readers saw every result exactly once", len(clean) == count, len(clean))
        r.check("The input offset moved only once", committed == count, committed)


@feature(
    "pipeline-audit", GROUP, "Audit the real billing pipeline",
    "Reads every ride event and every invoice straight from Kafka, then compares them one by one.",
    "Each completed trip must have exactly one invoice. Try it after killing billing mid-burst.",
    "docker kill billing-service     # while a burst is running\n"
    "docker start billing-service    # then run this audit",
)
async def pipeline_audit(r: Report) -> None:
    async with admin() as client:
        tps = [TopicPartition("ride-events", p) for p in range(6)]
        async with consumer() as c:
            async def caught_up():
                ends = await c.end_offsets(tps)
                saved = await client.list_consumer_group_offsets("billing")
                return all(saved[tp].offset >= ends[tp] for tp in tps if ends[tp]) and all(tp in saved for tp in tps if ends[tp])
            await kit.wait_until(caught_up, timeout=60, every=0.5, what="billing to catch up")

    rides = await read_topic("ride-events", partitions=6, limit=90)
    invoices = await read_topic("invoices", partitions=3, limit=90, isolation_level="read_committed")
    parked = await read_topic("ride-events-dlq", limit=30, isolation_level="read_committed")

    expected, broken = set(), set()
    for message in rides:
        place = (message.partition, message.offset)
        try:
            event = kit.loads(message.value)
            if event["event_type"] == "completed":
                expected.add(f"INV-{message.partition}-{message.offset}")
        except (ValueError, KeyError, TypeError):
            broken.add(place)

    issued = [kit.loads(m.value)["invoice_id"] for m in invoices]
    unique = set(issued)
    parked_from = {
        (s["partition"], s["offset"]) for s in (kit.loads(m.value)["source"] for m in parked)
    }
    r.table(
        ["Measure", "Count"],
        [
            ["Ride events in Kafka", len(rides)],
            ["Completed trips", len(expected)],
            ["Invoices visible to readers", len(issued)],
            ["Missing invoices", len(expected - unique)],
            ["Duplicate invoices", len(issued) - len(unique)],
            ["Broken messages", len(broken)],
            ["Parked in the dead letter topic", len(parked)],
        ],
    )
    r.check("Every completed trip has an invoice", not expected - unique, f"{len(expected - unique)} missing")
    r.check("No invoice appears twice", len(issued) == len(unique), f"{len(issued) - len(unique)} duplicates")
    r.check("No invoice exists without a completed trip", not unique - expected, f"{len(unique - expected)} extra")
    r.check("Every broken message was parked exactly once", broken == parked_from and len(parked) == len(broken), len(parked))


@feature(
    "fencing", GROUP, "Zombie fencing",
    "If a stalled instance comes back after its replacement started, Kafka refuses its writes.",
    "Two copies of a service can never both write as the same transactional id.",
    'AIOKafkaProducer(transactional_id="billing-1")',
)
async def fencing(r: Report) -> None:
    transaction_id = kit.unique("tx")
    async with topic("fence") as name:
        old = kit.producer(transactional_id=transaction_id)
        zombie = await old.__aenter__()
        await zombie.begin_transaction()
        await zombie.send_and_wait(name, value=b"from the old instance")
        r.step("The old instance opened a transaction, then stalled.")

        error = None
        async with producer(transactional_id=transaction_id) as replacement:
            r.step("A replacement started with the same transactional id.")
            try:
                await zombie.send_and_wait(name, value=b"zombie write")
                await zombie.commit_transaction()
            except KafkaError as exc:
                error = type(exc).__name__
            async with replacement.transaction():
                await replacement.send_and_wait(name, value=b"from the replacement")

        try:
            await old.__aexit__(None, None, None)
        except KafkaError:
            pass  # a fenced producer may also complain while closing

        visible = [m.value.decode() for m in await read_topic(name, isolation_level="read_committed")]
        r.check("Kafka rejected the zombie", error is not None, error)
        r.check("Only the replacement's write is visible", visible == ["from the replacement"], visible)
