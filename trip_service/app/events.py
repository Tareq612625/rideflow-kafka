"""The fake world: how trips and their events are built."""

import random
import uuid
from datetime import datetime, timezone

ZONES = [
    ("Gulshan", 0.18), ("Dhanmondi", 0.16), ("Uttara", 0.14), ("Mirpur", 0.13),
    ("Banani", 0.12), ("Motijheel", 0.10), ("Bashundhara", 0.09), ("Mohakhali", 0.08),
]
ZONE_NAMES = [z for z, _ in ZONES]
ZONE_WEIGHTS = [w for _, w in ZONES]

EVENT_TYPES = ["requested", "accepted", "completed", "cancelled"]
EVENT_WEIGHTS = [0.42, 0.27, 0.24, 0.07]

VEHICLES = ["bike", "cng", "car", "premium"]
VEHICLE_WEIGHTS = [0.45, 0.25, 0.22, 0.08]
VEHICLE_RATE = {"bike": 18.0, "cng": 24.0, "car": 32.0, "premium": 55.0}

CANCELLED_BEFORE_ACCEPT = 0.05
CANCELLED_AFTER_ACCEPT = 0.05


def _new_trip(driver_pool: int) -> dict:
    """What stays the same across every event of one trip."""
    vehicle = random.choices(VEHICLES, weights=VEHICLE_WEIGHTS)[0]
    distance = round(random.uniform(0.8, 24.0), 2)
    return {
        "trip_id": f"TRIP-{random.randint(100000, 999999)}",
        "driver_id": f"DRV-{random.randint(1, driver_pool):04d}",
        "rider_id": f"RID-{random.randint(1, driver_pool * 6):05d}",
        "zone": random.choices(ZONE_NAMES, weights=ZONE_WEIGHTS)[0],
        "vehicle": vehicle,
        "distance_km": distance,
        "total_fare": round(45 + distance * VEHICLE_RATE[vehicle] * random.uniform(0.85, 1.25), 2),
    }


def _event(trip: dict, event_type: str) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "trip_id": trip["trip_id"],
        "driver_id": trip["driver_id"],
        "rider_id": trip["rider_id"],
        "event_type": event_type,
        "zone": trip["zone"],
        "vehicle": trip["vehicle"],
        "distance_km": trip["distance_km"],
        "fare": trip["total_fare"] if event_type == "completed" else 0.0,
        "eta_min": random.randint(2, 18),
        "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
    }


def make_event(driver_pool: int = 250, event_type: str | None = None) -> dict:
    """One event on its own, for sending by hand."""
    chosen = event_type or random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0]
    return _event(_new_trip(driver_pool), chosen)


def trip_lifecycle(driver_pool: int = 250) -> list[dict]:
    """Every event of one trip, in order.

    They share a driver_id, which is the message key, so Kafka keeps them in one
    partition and a trip's events can never overtake each other.
    """
    trip = _new_trip(driver_pool)
    if random.random() < CANCELLED_BEFORE_ACCEPT:
        steps = ["requested", "cancelled"]
    elif random.random() < CANCELLED_AFTER_ACCEPT:
        steps = ["requested", "accepted", "cancelled"]
    else:
        steps = ["requested", "accepted", "completed"]
    return [_event(trip, step) for step in steps]
