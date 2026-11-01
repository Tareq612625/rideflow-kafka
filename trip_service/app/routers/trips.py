"""Endpoints that publish ride events into Kafka."""

from fastapi import APIRouter, HTTPException
from app.events import make_event
from app.producer import producer
from app.schemas import BrokenIn, SimulateIn, TripIn, TripOut

router = APIRouter(tags=["trips"])


@router.post("/trips", response_model=TripOut)
async def publish_one(body: TripIn | None = None):
    """Publish a single ride event and show exactly where Kafka stored it."""
    event = make_event(event_type=body.event_type if body else None)
    if body:
        if body.driver_id:
            event["driver_id"] = body.driver_id
        if body.zone:
            event["zone"] = body.zone

    metadata = await producer.send_one(event)
    return {
        "stored_in": {
            "topic": metadata.topic,
            "partition": metadata.partition,
            "offset": metadata.offset,
        },
        "event": event,
    }


@router.post("/trips/broken")
async def publish_broken(body: BrokenIn | None = None):
    """Publish a message no consumer can use, to see the dead letter queue work."""
    metadata = await producer.send_broken(body.kind if body else "missing-fields")
    return {"stored_in": {"partition": metadata.partition, "offset": metadata.offset}}


@router.post("/simulate")
async def simulate(body: SimulateIn):
    """Fire thousands of events in the background, as real trip lifecycles."""
    if not producer.start_burst(body.count, body.rate, body.drivers):
        raise HTTPException(409, "a burst is already running")
    return {"started": True, **body.model_dump()}


@router.post("/simulate/stop")
async def stop_simulation():
    stopped = producer.stop_burst()
    return {"stopped": stopped, "reason": None if stopped else "nothing running"}
