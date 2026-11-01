"""Stream processing by hand: event-time windows, joins and state in a changelog.

Kafka Streams itself is a Java library, so it is not used here. These are the
same three ideas in plain Python, running on real topics.
"""

import time
from collections import Counter, defaultdict
from app import kit
from app.kit import Report, producer, read_topic, topic
from app.registry import feature

GROUP = "Stream processing"


@feature(
    "windows", GROUP, "Event-time windows and late data",
    "Count events in 10-second windows, using the time inside each event, not the time it arrived.",
    "Network delays reorder events. Event time still gives the right answer, and data too late is dropped on purpose.",
    "window = event_time - event_time % 10_000\nwatermark = newest_event_time - grace",
)
async def windows(r: Report) -> None:
    window_ms, grace_ms = 10_000, 5_000
    base = int(time.time() * 1000) // window_ms * window_ms - 60_000

    # seconds into the stream, in the order the events ARRIVE
    arrival_order = [1, 3, 12, 2, 9, 14, 21, 23, 28, 41, 4]
    async with topic("windows") as name, producer(acks="all") as p:
        for second in arrival_order:
            await p.send_and_wait(name, value=kit.dumps({"ts": base + second * 1000}))

        counts, late, closed = Counter(), [], set()
        watermark = 0
        for message in await read_topic(name):
            event_time = kit.loads(message.value)["ts"]
            window = event_time - event_time % window_ms
            if window in closed:
                late.append((event_time - base) // 1000)
                continue
            counts[window] += 1
            watermark = max(watermark, event_time - grace_ms)
            closed.update(w for w in counts if w + window_ms <= watermark)

        r.step(f"Events arrived in this order, by their own second: {arrival_order}")
        r.table(
            ["Window", "Events counted"],
            [[f"{(w - base) // 1000}-{(w - base) // 1000 + 10} s", n] for w, n in sorted(counts.items())],
        )
        result = {(w - base) // 1000: n for w, n in counts.items()}
        r.check("Out-of-order events landed in the right window", result == {0: 4, 10: 2, 20: 3, 40: 1}, result)
        r.check("The event 37 seconds late was dropped as too late", late == [4], late)


@feature(
    "stream-table-join", GROUP, "Stream-table join",
    "A compacted topic holds each driver's profile. Each ride joins with the newest profile.",
    "This is enrichment, and it uses a topic as a live lookup table. A profile change applies to later rides.",
    'ride["rating"] = drivers[ride["driver_id"]]["rating"]',
)
async def stream_table_join(r: Report) -> None:
    async with topic("drivers", configs={"cleanup.policy": "compact"}) as drivers, topic("rides") as rides:
        async with producer(acks="all") as p:
            await p.send_and_wait(drivers, key=b"DRV-7", value=kit.dumps({"name": "Rahim", "rating": 4.1}))
            await p.send_and_wait(drivers, key=b"DRV-9", value=kit.dumps({"name": "Karim", "rating": 4.8}))

            async def load_table() -> dict:
                table = {}
                for message in await read_topic(drivers):
                    table[message.key] = kit.loads(message.value)
                return table

            await p.send_and_wait(rides, key=b"DRV-7", value=kit.dumps({"trip": "T-1", "driver": "DRV-7"}))
            await p.send_and_wait(rides, key=b"DRV-9", value=kit.dumps({"trip": "T-2", "driver": "DRV-9"}))

            table = await load_table()
            joined = []
            for message in await read_topic(rides):
                ride = kit.loads(message.value)
                joined.append([ride["trip"], table[message.key]["name"], table[message.key]["rating"]])

            await p.send_and_wait(drivers, key=b"DRV-7", value=kit.dumps({"name": "Rahim", "rating": 3.2}))
            r.step("Rahim's rating changed from 4.1 to 3.2 in the table topic.")
            await p.send_and_wait(rides, key=b"DRV-7", value=kit.dumps({"trip": "T-3", "driver": "DRV-7"}))

            table = await load_table()
            ride = kit.loads((await read_topic(rides))[-1].value)
            joined.append([ride["trip"], table[b"DRV-7"]["name"], table[b"DRV-7"]["rating"]])

        r.table(["Trip", "Driver", "Rating at join time"], joined)
        r.check("Rides were enriched from the table", joined[0][1] == "Rahim" and joined[1][1] == "Karim")
        r.check("A later ride saw the updated rating", joined[0][2] == 4.1 and joined[2][2] == 3.2)


@feature(
    "changelog", GROUP, "State in a changelog topic",
    "Every state change is written to a compacted topic. After a crash, state is rebuilt by reading it back.",
    "This is how Kafka Streams keeps state safe, and RideFlow's dashboards now survive restarts the same way.",
    'await producer.send("analytics-state", key=group_id, value=snapshot)',
)
async def changelog(r: Report) -> None:
    zones = ["Gulshan", "Uttara", "Gulshan", "Mirpur", "Gulshan", "Uttara"]
    async with topic("changelog", configs={"cleanup.policy": "compact"}) as name, producer(acks="all") as p:
        state: dict[str, int] = defaultdict(int)
        for zone in zones:
            state[zone] += 1
            await p.send_and_wait(name, key=zone.encode(), value=kit.dumps(state[zone]))
        before = dict(state)
        r.step(f"Counted {len(zones)} events. In-memory state: {before}")

        del state
        r.step("The process crashed, so the in-memory state is gone.")

        rebuilt = {}
        for message in await read_topic(name):
            rebuilt[message.key.decode()] = kit.loads(message.value)
        r.step(f"Rebuilt by reading the changelog: {rebuilt}")
        r.check("Rebuilt state exactly equals the state before the crash", rebuilt == before)
