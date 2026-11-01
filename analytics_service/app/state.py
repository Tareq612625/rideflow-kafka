"""
All the numbers the dashboard shows.

They live in memory for speed, and checkpoint.py saves them to a compacted Kafka
topic, so they survive a restart.
"""

import bisect
import json
from collections import defaultdict, deque
from datetime import datetime, timezone
from app import config

SURGE_WINDOW_SEC = 3.0
SURGE_ALERT_GAP_SEC = 5.0


def _zone():
    return {"events": 0, "trips": 0, "revenue": 0.0}


def _driver():
    return {"trips": 0, "revenue": 0.0}


class Analytics:
    def __init__(self) -> None:
        self.lag = 0
        self.connected = False
        self.throttle_ms = 0.0
        self._clear()

    def _clear(self) -> None:
        self.consumed = 0
        self.completed = 0
        self.cancelled = 0
        self.bad = 0
        self.revenue = 0.0
        self.needs_saving = False

        self.partitions: dict[int, dict] = {}
        self.by_zone: dict[str, dict] = defaultdict(_zone)
        self.by_type: dict[str, int] = defaultdict(int)
        self.drivers: dict[str, dict] = defaultdict(_driver)
        self.alerts: deque = deque(maxlen=10)
        self.recent: deque = deque(maxlen=22)

        self._zone_window: dict[str, list] = defaultdict(list)
        self._zone_last_alert: dict[str, float] = defaultdict(float)

    def record(self, msg) -> None:
        """The 'business logic': one Kafka message in, statistics updated.

        Raises on a malformed message; the caller sends it to the skip pile
        rather than letting it stop the partition.
        """
        event = json.loads(msg.value)

        # Read every field we need BEFORE touching any counter. If the message
        # is missing something, this raises while the statistics are still
        # untouched — a half-processed message would corrupt the totals.
        event_type = event["event_type"]
        zone_name = event["zone"]
        driver_id = event["driver_id"]
        vehicle = event["vehicle"]
        fare = float(event["fare"])
        km = event["distance_km"]
        event_time = datetime.fromisoformat(event["ts"]).timestamp()
        key = msg.key.decode("utf-8") if msg.key else None

        part = self.partitions.setdefault(
            msg.partition, {"count": 0, "last_offset": -1, "lag": 0}
        )
        part["count"] += 1
        part["last_offset"] = msg.offset

        self.consumed += 1
        self.by_type[event_type] += 1
        zone = self.by_zone[zone_name]
        zone["events"] += 1

        if event_type == "completed":
            self.completed += 1
            self.revenue += fare
            zone["trips"] += 1
            zone["revenue"] += fare
            driver = self.drivers[driver_id]
            driver["trips"] += 1
            driver["revenue"] += fare
        elif event_type == "cancelled":
            self.cancelled += 1
        elif event_type == "requested":
            self._check_surge(zone_name, event_time)

        self.recent.appendleft(
            {
                "type": event_type,
                "zone": zone_name,
                "driver": driver_id,
                "vehicle": vehicle,
                "fare": fare,
                "km": km,
                "partition": msg.partition,
                "offset": msg.offset,
                "key": key,
                "timestamp": msg.timestamp,
                "bytes": msg.serialized_value_size,
            }
        )

    def _check_surge(self, zone_name: str, event_time: float) -> None:
        """Stateful stream processing: too many requests in 3s -> surge pricing.

        It uses the time inside the event, not the time it was read. Replaying a
        day of history in seconds therefore raises no false alarms, because the
        events still say when they really happened.
        """
        window = self._zone_window[zone_name]
        bisect.insort(window, event_time)   # different partitions arrive out of order
        newest = window[-1]
        del window[: bisect.bisect_left(window, newest - SURGE_WINDOW_SEC)]

        if (
            len(window) >= config.SURGE_REQUESTS_PER_3S
            and newest - self._zone_last_alert[zone_name] > SURGE_ALERT_GAP_SEC
        ):
            self._zone_last_alert[zone_name] = newest
            self.alerts.appendleft(
                {
                    "zone": zone_name,
                    "requests": len(window),
                    "multiplier": round(1.0 + len(window) / 400.0, 1),
                    "at": datetime.fromtimestamp(newest, timezone.utc).strftime("%H:%M:%S"),
                }
            )

    def set_lag(self, partition: int, lag: int) -> None:
        part = self.partitions.setdefault(
            partition, {"count": 0, "last_offset": -1, "lag": 0}
        )
        part["lag"] = lag

    def snapshot(self) -> dict:
        zones = sorted(
            ({"zone": name, **data} for name, data in self.by_zone.items()),
            key=lambda z: z["events"],
            reverse=True,
        )[:8]
        drivers = sorted(
            ({"driver": name, **data} for name, data in self.drivers.items()),
            key=lambda d: d["revenue"],
            reverse=True,
        )[:5]
        partitions = [
            {"partition": pid, **data} for pid, data in sorted(self.partitions.items())
        ]
        return {
            "consumed": self.consumed,
            "completed": self.completed,
            "cancelled": self.cancelled,
            "bad": self.bad,
            "revenue": round(self.revenue, 2),
            "lag": self.lag,
            "connected": self.connected,
            "throttle_ms": self.throttle_ms,
            "group_id": config.GROUP_ID,
            "partitions": partitions,
            "zones": zones,
            "drivers": drivers,
            "by_type": dict(self.by_type),
            "alerts": list(self.alerts),
            "recent": list(self.recent),
        }

    def dump(self) -> dict:
        """Everything needed to carry on after a restart."""
        return {
            "consumed": self.consumed,
            "completed": self.completed,
            "cancelled": self.cancelled,
            "bad": self.bad,
            "revenue": self.revenue,
            "partitions": {str(k): v for k, v in self.partitions.items()},
            "by_zone": dict(self.by_zone),
            "by_type": dict(self.by_type),
            "drivers": dict(self.drivers),
            "alerts": list(self.alerts),
            "recent": list(self.recent),
        }

    def restore(self, saved: dict | None) -> None:
        self._clear()
        if not saved:
            return
        self.consumed = saved["consumed"]
        self.completed = saved["completed"]
        self.cancelled = saved["cancelled"]
        self.bad = saved["bad"]
        self.revenue = saved["revenue"]
        self.partitions = {int(k): v for k, v in saved["partitions"].items()}
        self.by_zone.update(saved["by_zone"])
        self.by_type.update(saved["by_type"])
        self.drivers.update(saved["drivers"])
        self.alerts.extend(saved["alerts"])
        self.recent.extend(saved["recent"])

    def reset(self) -> None:
        """Clears the dashboard only. Kafka still holds every message."""
        partitions = self.partitions
        self._clear()
        self.partitions = partitions
        for part in self.partitions.values():
            part["count"] = 0
        self.needs_saving = True


analytics = Analytics()
