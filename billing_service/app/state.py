"""Everything the billing dashboard shows.

It lives in memory for speed, and is saved to a compacted Kafka topic inside the
same transaction as the invoices, so it survives a restart.
"""

from collections import defaultdict, deque
from app import config


def _driver():
    return {"trips": 0, "payout": 0.0}


class Billing:
    def __init__(self) -> None:
        self.lag = 0
        self.connected = False
        self._clear()

    def _clear(self) -> None:
        self.seen = 0
        self.skipped = 0
        self.poisoned = 0
        self.invoices = 0
        self.gross = 0.0
        self.commission = 0.0
        self.payout = 0.0
        self.needs_saving = False

        self.partitions: dict[int, dict] = {}
        self.by_driver: dict[str, dict] = defaultdict(_driver)
        self.by_zone: dict[str, float] = defaultdict(float)
        self.recent: deque = deque(maxlen=20)

    def skip(self) -> None:
        self.seen += 1
        self.skipped += 1

    def poison(self) -> None:
        self.seen += 1
        self.poisoned += 1

    def record(self, invoice: dict) -> None:
        self.seen += 1
        self.invoices += 1
        self.gross += invoice["fare"]
        self.commission += invoice["commission"]
        self.payout += invoice["driver_payout"]

        driver = self.by_driver[invoice["driver_id"]]
        driver["trips"] += 1
        driver["payout"] += invoice["driver_payout"]
        self.by_zone[invoice["zone"]] += invoice["fare"]

        self.recent.appendleft(invoice)

    def touch_partition(self, partition: int, offset: int) -> None:
        part = self.partitions.setdefault(partition, {"count": 0, "last_offset": -1, "lag": 0})
        part["count"] += 1
        part["last_offset"] = offset

    def set_lag(self, partition: int, lag: int) -> None:
        self.partitions.setdefault(partition, {"count": 0, "last_offset": -1, "lag": 0})["lag"] = lag

    def snapshot(self) -> dict:
        drivers = sorted(
            ({"driver": k, **v} for k, v in self.by_driver.items()),
            key=lambda d: d["payout"], reverse=True,
        )[:6]
        zones = sorted(
            ({"zone": k, "gross": v} for k, v in self.by_zone.items()),
            key=lambda z: z["gross"], reverse=True,
        )[:6]
        return {
            "seen": self.seen,
            "skipped": self.skipped,
            "poisoned": self.poisoned,
            "dlq_topic": config.DLQ_TOPIC,
            "invoices": self.invoices,
            "gross": round(self.gross, 2),
            "commission": round(self.commission, 2),
            "payout": round(self.payout, 2),
            "lag": self.lag,
            "connected": self.connected,
            "group_id": config.GROUP_ID,
            "source_topic": config.SOURCE_TOPIC,
            "invoice_topic": config.INVOICE_TOPIC,
            "commission_rate": config.COMMISSION_RATE,
            "partitions": [{"partition": p, **d} for p, d in sorted(self.partitions.items())],
            "drivers": drivers,
            "zones": zones,
            "recent": list(self.recent),
        }

    def dump(self) -> dict:
        """Everything needed to carry on after a restart."""
        return {
            "seen": self.seen,
            "skipped": self.skipped,
            "poisoned": self.poisoned,
            "invoices": self.invoices,
            "gross": self.gross,
            "commission": self.commission,
            "payout": self.payout,
            "partitions": {str(k): v for k, v in self.partitions.items()},
            "by_driver": dict(self.by_driver),
            "by_zone": dict(self.by_zone),
            "recent": list(self.recent),
        }

    def restore(self, saved: dict | None) -> None:
        self._clear()
        if not saved:
            return
        self.seen = saved["seen"]
        self.skipped = saved["skipped"]
        self.poisoned = saved["poisoned"]
        self.invoices = saved["invoices"]
        self.gross = saved["gross"]
        self.commission = saved["commission"]
        self.payout = saved["payout"]
        self.partitions = {int(k): v for k, v in saved["partitions"].items()}
        self.by_driver.update(saved["by_driver"])
        self.by_zone.update(saved["by_zone"])
        self.recent.extend(saved["recent"])

    def reset(self) -> None:
        partitions = self.partitions
        self._clear()
        self.partitions = partitions
        for part in self.partitions.values():
            part["count"] = 0
        self.needs_saving = True


billing = Billing()
