"""Stats, health and the consumer-speed control."""

from fastapi import APIRouter
from app import config
from app.schemas import ThrottleIn
from app.state import analytics

router = APIRouter(tags=["system"])


@router.get("/stats")
async def stats():
    return analytics.snapshot()


@router.post("/throttle")
async def set_throttle(body: ThrottleIn):
    """Slow the consumer down on purpose, so lag becomes visible."""
    analytics.throttle_ms = body.ms
    return {"throttle_ms": analytics.throttle_ms}


@router.post("/reset")
async def reset():
    analytics.reset()
    return {"reset": True}


@router.get("/health")
async def health():
    return {
        "service": config.SERVICE_NAME,
        "role": "consumer",
        "kafka": config.KAFKA_BOOTSTRAP_SERVERS,
        "topic": config.TOPIC,
        "group_id": config.GROUP_ID,
        "connected": analytics.connected,
        "consumed": analytics.consumed,
    }
