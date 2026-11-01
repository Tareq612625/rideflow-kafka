"""How messages come out: groups, rebalancing, commits, seeking, lag, DLQ and retries."""

import asyncio
import time
from aiokafka.coordinator.assignors.range import RangePartitionAssignor
from aiokafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from aiokafka.coordinator.assignors.sticky.sticky_assignor import StickyPartitionAssignor
from aiokafka.errors import KafkaError
from aiokafka.structs import OffsetAndMetadata, TopicPartition
from app import kit
from app.kit import Report, admin, consumer, group_assignment, producer, read, read_topic, topic, wait_until
from app.registry import feature

GROUP = "Consumers"

# fast heartbeats, so that rebalances finish in about a second
QUICK = {"heartbeat_interval_ms": 300, "session_timeout_ms": 6000}


async def fill(name: str, count: int, partitions: int = 1) -> None:
    async with producer(acks="all") as p:
        for i in range(count):
            await p.send_and_wait(name, value=kit.dumps({"n": i}), partition=i % partitions)


async def read_shared(members: list, want: int, limit: float = 20.0) -> list:
    """Several group members read at once until `want` messages arrived between them."""
    received: list = []
    deadline = time.monotonic() + limit
    while len(received) < want and time.monotonic() < deadline:
        for batches in await asyncio.gather(*(m.getmany(timeout_ms=200) for m in members)):
            received.extend(msg for messages in batches.values() for msg in messages)
    return received


@feature(
    "group-share", GROUP, "A consumer group shares the work",
    "Consumers with the same group id split the partitions between them. Each partition has one owner.",
    "This is how you scale reading: start more copies of your service.",
    'AIOKafkaConsumer("orders", group_id="billing")   # run this 3 times',
)
async def group_share(r: Report) -> None:
    async with topic("share", partitions=6) as name:
        await fill(name, 60, partitions=6)
        options = dict(group_id=kit.unique("group"), **QUICK)
        async with consumer(name, **options) as a, consumer(name, **options) as b, consumer(name, **options) as c:
            members = [a, b, c]
            await wait_until(
                lambda: [len(m.assignment()) for m in members] == [2, 2, 2],
                timeout=30, what="the group to balance",
            )
            owned = [await group_assignment(m) for m in members]
            r.table(
                ["Consumer", "Partitions it owns"],
                [[f"Consumer {i + 1}", [f"P{p}" for p in o]] for i, o in enumerate(owned)],
            )
            everything = [p for o in owned for p in o]
            r.check("Each consumer owns 2 of the 6 partitions", all(len(o) == 2 for o in owned))
            r.check("No partition has two owners", sorted(everything) == list(range(6)))
            received = await read_shared(members, 60)
            r.check("Between them they read all 60 messages exactly once", len(received) == 60, len(received))


@feature(
    "rebalance", GROUP, "Rebalancing when a consumer leaves",
    "When a member leaves, Kafka moves its partitions to the others.",
    "This gives you automatic failover for readers, with no human involved.",
    "# nothing to write: the group coordinator does this for you",
)
async def rebalance(r: Report) -> None:
    async with topic("rebalance", partitions=6) as name:
        options = dict(group_id=kit.unique("group"), **QUICK)
        async with consumer(name, **options) as a, consumer(name, **options) as b, consumer(name, **options) as c:
            await wait_until(lambda: [len(m.assignment()) for m in (a, b, c)] == [2, 2, 2], timeout=30, what="the group")
            before = [await group_assignment(m) for m in (a, b, c)]

            await b.stop()
            r.step(f"Consumer 2 left the group. It owned partitions {before[1]}.")
            started = time.monotonic()
            await wait_until(lambda: len(a.assignment()) + len(c.assignment()) == 6, timeout=20, what="the rebalance")
            took = time.monotonic() - started
            after = [await group_assignment(m) for m in (a, c)]
            r.table(
                ["Consumer", "Before", "After"],
                [
                    ["Consumer 1", before[0], after[0]],
                    ["Consumer 2", before[1], "left the group"],
                    ["Consumer 3", before[2], after[1]],
                ],
            )
            r.check("The survivors took over all 6 partitions", sorted(after[0] + after[1]) == list(range(6)), f"in {took:.1f} s")
            await fill(name, 30, partitions=6)
            received = await read_shared([a, c], 30)
            r.check("No message was lost", len(received) == 30, len(received))


@feature(
    "pub-sub", GROUP, "Two groups each get every message",
    "Each group keeps its own offsets, so reading in one group has no effect on another.",
    "This is why billing and analytics can both read ride-events without knowing about each other.",
    'AIOKafkaConsumer("rides", group_id="analytics")\nAIOKafkaConsumer("rides", group_id="billing")',
)
async def pub_sub(r: Report) -> None:
    async with topic("pubsub", partitions=3) as name:
        await fill(name, 30, partitions=3)
        async with consumer(name, group_id=kit.unique("analytics")) as a, consumer(name, group_id=kit.unique("billing")) as b:
            first, second = await asyncio.gather(read(a, want=30), read(b, want=30))
            r.table(["Group", "Messages received"], [["analytics", len(first)], ["billing", len(second)]])
            r.check("Each group received all 30 messages", len(first) == 30 and len(second) == 30)


@feature(
    "assignors", GROUP, "Assignment strategies",
    "The assignor decides how partitions are divided between group members.",
    "The default choice can be uneven. Sticky also keeps partitions where they are during a rebalance.",
    "partition_assignment_strategy=(StickyPartitionAssignor,)",
)
async def assignors(r: Report) -> None:
    strategies = {
        "range": RangePartitionAssignor,
        "roundrobin": RoundRobinPartitionAssignor,
        "sticky": StickyPartitionAssignor,
    }
    rows, sizes = [], {}
    async with topic("assign-a", partitions=3) as first, topic("assign-b", partitions=3) as second:
        for label, strategy in strategies.items():
            options = dict(group_id=kit.unique(label), partition_assignment_strategy=(strategy,), **QUICK)
            async with consumer(first, second, **options) as a, consumer(first, second, **options) as b:
                await wait_until(lambda: len(a.assignment()) + len(b.assignment()) == 6, timeout=30, what=label)
                sizes[label] = sorted([len(a.assignment()), len(b.assignment())])
                rows.append([label, len(a.assignment()), len(b.assignment())])
    r.table(["Strategy", "Consumer 1 partitions", "Consumer 2 partitions"], rows, "Two topics with 3 partitions each")
    r.check("Range is uneven here (4 and 2)", sizes["range"] == [2, 4], sizes["range"])
    r.check("Round robin is even (3 and 3)", sizes["roundrobin"] == [3, 3], sizes["roundrobin"])
    r.check("Sticky is even (3 and 3)", sizes["sticky"] == [3, 3], sizes["sticky"])


@feature(
    "static-membership", GROUP, "Static membership",
    "A consumer with a fixed instance id can restart without causing a rebalance.",
    "Rolling restarts stop disturbing the whole group, because the member is expected back.",
    'AIOKafkaConsumer(topic, group_id="billing", group_instance_id="billing-1")',
)
async def static_membership(r: Report) -> None:
    async with topic("static", partitions=4) as name:
        async def restart_effect(a_id, b_id) -> int:
            options = dict(group_id=kit.unique("group"), **QUICK)
            async with consumer(name, group_instance_id=b_id, **options) as other:
                restarting = kit.consumer(name, group_instance_id=a_id, **options)
                client = await restarting.__aenter__()
                await wait_until(lambda: len(other.assignment()) == 2 and len(client.assignment()) == 2, timeout=30, what="the group")
                await asyncio.sleep(1)
                before = other._coordinator.generation
                await restarting.__aexit__(None, None, None)
                async with consumer(name, group_instance_id=a_id, **options) as again:
                    await wait_until(lambda: len(again.assignment()) == 2, timeout=30, what="the rejoin")
                    await asyncio.sleep(1.5)
                    return other._coordinator.generation - before

        normal = await restart_effect(None, None)
        fixed = await restart_effect("lab-static-a", "lab-static-b")
        r.table(
            ["Consumer type", "Rebalances caused by one restart"],
            [["Normal member", normal], ["Static member", fixed]],
        )
        r.check("A normal restart caused a rebalance", normal >= 1, normal)
        r.check("A static member restart caused none", fixed == 0, fixed)


@feature(
    "offset-reset", GROUP, "auto_offset_reset: where a new group starts",
    "A group with no saved position needs a starting point: the oldest message, only new messages, or an error.",
    "This is what lets a brand new service replay all of history, or skip it.",
    'AIOKafkaConsumer(topic, group_id="new", auto_offset_reset="earliest")',
)
async def offset_reset(r: Report) -> None:
    tp_partitions = 1
    async with topic("reset") as name:
        await fill(name, 5)
        async with consumer(name, group_id=kit.unique("early"), auto_offset_reset="earliest") as c:
            earliest = await read(c, want=5)

        async with consumer(name, group_id=kit.unique("late"), auto_offset_reset="latest") as c:
            await wait_until(lambda: len(c.assignment()) == tp_partitions, timeout=20, what="assignment")
            tp = TopicPartition(name, 0)
            await c.position(tp)
            await fill(name, 1)
            latest = await read(c, want=1)

        error = None
        async with consumer(name, group_id=kit.unique("none"), auto_offset_reset="none") as c:
            await wait_until(lambda: len(c.assignment()) == tp_partitions, timeout=20, what="assignment")
            try:
                await asyncio.wait_for(c.getmany(timeout_ms=3000), 10)
            except KafkaError as exc:
                error = type(exc).__name__

        r.table(
            ["Setting", "Result"],
            [
                ["earliest", f"read {len(earliest)} old messages"],
                ["latest", f"skipped the 5 old messages, read {len(latest)} new one"],
                ["none", f"raised {error}"],
            ],
        )
        r.check("earliest read all the history", len(earliest) == 5)
        r.check("latest read only the new message", len(latest) == 1)
        r.check("none raised an error", error is not None, error)


@feature(
    "delivery", GROUP, "At-most-once and at-least-once",
    "The order of working and committing decides what a crash does.",
    "Commit first and a crash loses messages. Work first and a crash repeats them. Most systems choose repeats.",
    "await work(batch)\nawait consumer.commit()   # at-least-once",
)
async def delivery(r: Report) -> None:
    async def scenario(commit_first: bool) -> tuple[list[int], list[int]]:
        async with topic("delivery") as name:
            await fill(name, 10)
            group = kit.unique("group")
            done = []
            async with consumer(name, group_id=group) as c:
                batch = await read(c, want=10)
                if commit_first:
                    await c.commit()
                for message in batch[:6]:
                    done.append(kit.loads(message.value)["n"])
                # the crash happens here: 6 of 10 finished, and no more code runs
            first_run = list(done)
            async with consumer(name, group_id=group) as c:
                for message in await read(c, want=None if commit_first else 10, quiet_ms=1500):
                    done.append(kit.loads(message.value)["n"])
            return first_run, done

    _, most = await scenario(commit_first=True)
    _, least = await scenario(commit_first=False)
    lost = sorted(set(range(10)) - set(most))
    repeated = sorted({n for n in least if least.count(n) > 1})
    r.step("Each run reads 10 messages, then crashes after finishing 6 of them, then restarts.")
    r.table(
        ["Order", "Handled", "Lost", "Repeated"],
        [
            ["Commit, then work", len(set(most)), lost, []],
            ["Work, then commit", len(set(least)), [], repeated],
        ],
    )
    r.check("At-most-once lost the 4 unfinished messages", lost == [6, 7, 8, 9], lost)
    r.check("At-least-once lost nothing", set(least) == set(range(10)))
    r.check("At-least-once repeated the 6 finished messages", repeated == [0, 1, 2, 3, 4, 5], repeated)


@feature(
    "seek", GROUP, "Seek by offset and by time",
    "A consumer can jump to any stored offset, or find the first message after a point in time.",
    "Time seeking is how you replay exactly the data from one bad hour.",
    "consumer.seek(tp, 7)\nawait consumer.offsets_for_times({tp: timestamp_ms})",
)
async def seek(r: Report) -> None:
    base = int((time.time() - 3600) * 1000)
    async with topic("seek") as name:
        async with producer(acks="all") as p:
            for i in range(20):
                await p.send_and_wait(name, value=kit.dumps({"n": i}), timestamp_ms=base + i * 1000)
        tp = TopicPartition(name, 0)
        async with consumer() as c:
            c.assign([tp])
            c.seek(tp, 7)
            at = (await read(c, want=1))[0]
            r.check("seek(7) read offset 7", kit.loads(at.value)["n"] == 7 and at.offset == 7, at.offset)

            await c.seek_to_beginning(tp)
            first = (await read(c, want=1))[0]
            r.check("seek_to_beginning read offset 0", first.offset == 0, first.offset)

            await c.seek_to_end(tp)
            position = await c.position(tp)
            r.check("seek_to_end moved past the last message", position == 20, position)

            found = await c.offsets_for_times({tp: base + 12_000})
            r.step(f"Asked for the first message at or after 12 seconds in: Kafka answered offset {found[tp].offset}.")
            c.seek(tp, found[tp].offset)
            hit = (await read(c, want=1))[0]
            r.check("Searching by time found the right message", kit.loads(hit.value)["n"] == 12, hit.offset)


@feature(
    "reset-offsets", GROUP, "Rewind a group",
    "You can move a group's saved position backwards, so it reads old messages again.",
    "After fixing a bug, rewind the group and reprocess the data it handled badly.",
    "await consumer.commit({tp: OffsetAndMetadata(3, '')})",
)
async def reset_offsets(r: Report) -> None:
    async with topic("rewind") as name:
        await fill(name, 10)
        group = kit.unique("group")
        tp = TopicPartition(name, 0)
        async with consumer(name, group_id=group) as c:
            await read(c, want=10)
            await c.commit()
        async with admin() as a:
            saved = await a.list_consumer_group_offsets(group)
        r.check("The group had finished all 10 messages", saved[tp].offset == 10, saved[tp].offset)

        async with consumer(group_id=group) as c:
            c.assign([tp])
            await c.commit({tp: OffsetAndMetadata(3, "")})
        r.step("Moved the group's saved position back to offset 3.")

        async with consumer(name, group_id=group) as c:
            again = await read(c, want=7)
        r.check("The group re-read offsets 3 to 9", [m.offset for m in again] == list(range(3, 10)), [m.offset for m in again])


@feature(
    "pause-resume", GROUP, "Pause and resume a partition",
    "A consumer can stop fetching one partition while it keeps reading the others, and stay in the group.",
    "Use it for back-pressure, when the system behind one partition is busy.",
    "consumer.pause(tp)\nconsumer.resume(tp)",
)
async def pause_resume(r: Report) -> None:
    async with topic("pause", partitions=2) as name:
        await fill(name, 10, partitions=2)
        p0, p1 = TopicPartition(name, 0), TopicPartition(name, 1)
        async with consumer() as c:
            c.assign([p0, p1])
            await c.seek_to_beginning(p0, p1)
            c.pause(p0)
            while_paused = await read(c, quiet_ms=1500)
            r.check("While P0 was paused, only P1 was read", {m.partition for m in while_paused} == {1}, len(while_paused))
            c.resume(p0)
            after = await read(c, want=5)
            r.check("After resume, P0 delivered its 5 messages", {m.partition for m in after} == {0} and len(after) == 5, len(after))


@feature(
    "lag", GROUP, "Consumer lag",
    "Lag is how many messages a group has not finished yet: end offset minus committed offset.",
    "It is the most important number to monitor. Rising lag means the reader is too slow.",
    "lag = end_offset - committed_offset",
)
async def lag(r: Report) -> None:
    async with topic("lag") as name:
        await fill(name, 100)
        group = kit.unique("group")
        tp = TopicPartition(name, 0)
        async with consumer(name, group_id=group) as c:
            await read(c, want=100)
            await c.commit({tp: 30})
            end = (await c.end_offsets([tp]))[tp]
        async with admin() as a:
            committed = (await a.list_consumer_group_offsets(group))[tp].offset
        r.table(["End offset", "Committed offset", "Lag"], [[end, committed, end - committed]])
        r.check("Lag is 70: 100 written, 30 finished", end - committed == 70, end - committed)


@feature(
    "dead-letter", GROUP, "Dead letter queue",
    "A message that cannot be processed is parked in another topic, and the stream keeps moving.",
    "Without this, one broken record crashes the consumer on the same message forever.",
    'except ValueError as bad:\n    await producer.send("orders-dlq", value=message.value)',
)
async def dead_letter(r: Report) -> None:
    messages = [kit.dumps({"n": i}) for i in range(10)]
    messages[4] = b"{this is not json"
    async with topic("stream") as name, topic("stream-dlq") as dlq, producer(acks="all") as p:
        for value in messages:
            await p.send_and_wait(name, value=value)

        handled = []
        for message in await read_topic(name):
            try:
                handled.append(kit.loads(message.value)["n"])
            except ValueError as exc:
                await p.send_and_wait(
                    dlq, value=message.value,
                    headers=[("error", str(exc).encode()), ("source-offset", str(message.offset).encode())],
                )
        parked = await read_topic(dlq)
        r.check("The 9 good messages were processed", len(handled) == 9, handled)
        r.check("The broken one was parked, with the reason", len(parked) == 1, dict(parked[0].headers).get("source-offset"))

        fixed = parked[0].value.replace(b"{this is not json", kit.dumps({"n": 4}))
        await p.send_and_wait(name, value=fixed)
        replayed = (await read_topic(name))[-1]
        r.check("After a fix, the parked message was replayed", kit.loads(replayed.value)["n"] == 4)


@feature(
    "retry-topic", GROUP, "Retry topics with a delay",
    "A temporary failure goes to a retry topic with a time to try again, so the main stream never stops.",
    "A database outage should delay some messages, not block all of them.",
    'headers=[("retry-at", str(now + delay).encode())]',
)
async def retry_topic(r: Report) -> None:
    delay_ms = 1500
    async with topic("work") as name, topic("work-retry") as retry, producer(acks="all") as p:
        for i in range(3):
            await p.send_and_wait(name, value=kit.dumps({"n": i}))

        database_up = False
        done, delayed = [], []
        started = time.monotonic()
        for message in await read_topic(name):
            n = kit.loads(message.value)["n"]
            if n == 1 and not database_up:
                due = int(time.time() * 1000) + delay_ms
                await p.send_and_wait(retry, value=message.value, headers=[("retry-at", str(due).encode())])
                delayed.append(n)
            else:
                done.append(n)
        r.check("Messages 0 and 2 did not wait for the failed one", done == [0, 2], done)

        database_up = True
        for message in await read_topic(retry):
            due = int(dict(message.headers)["retry-at"])
            await asyncio.sleep(max(0, due / 1000 - time.time()))
            done.append(kit.loads(message.value)["n"])
        waited = time.monotonic() - started
        r.check("The failed message succeeded after its delay", sorted(done) == [0, 1, 2] and waited >= delay_ms / 1000, f"{waited:.1f} s")
