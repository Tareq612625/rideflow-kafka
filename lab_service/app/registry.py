"""Every feature registers itself here, and this runs them safely, one at a time."""

import asyncio
import time
import traceback
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from app import config
from app.kit import Report, Skip

GROUPS = [
    "Topics and messages",
    "Producers",
    "Consumers",
    "Transactions and exactly-once",
    "Storage and retention",
    "The cluster",
    "Stream processing",
    "Security",
    "Ecosystem",
]


@dataclass
class Feature:
    id: str
    group: str
    title: str
    what: str
    why: str
    code: str
    run: Callable[[Report], Awaitable[None]]
    optional: bool = False


FEATURES: dict[str, Feature] = {}


def feature(id: str, group: str, title: str, what: str, why: str, code: str = "", optional: bool = False):
    def register(run):
        assert group in GROUPS, group
        FEATURES[id] = Feature(id, group, title, what, why, code, run, optional)
        return run

    return register


# One run at a time, so timing measurements are honest and logs stay readable.
_one_at_a_time = asyncio.Lock()


async def run_feature(feature_id: str, options: dict | None = None) -> dict:
    item = FEATURES[feature_id]
    report = Report(options)
    started = time.monotonic()
    error = None

    async with _one_at_a_time:
        try:
            await asyncio.wait_for(item.run(report), timeout=config.RUN_TIMEOUT_SEC)
            status = "passed" if report.passed else "failed"
        except Skip as reason:
            status = "skipped"
            error = str(reason)
        except Exception as exc:  # a failed run must be reported, never crash the Lab
            status = "failed"
            error = f"{type(exc).__name__}: {exc}"
            traceback.print_exc()

    return {
        "id": item.id,
        "status": status,
        "seconds": round(time.monotonic() - started, 2),
        "error": error,
        "items": report.items,
    }
