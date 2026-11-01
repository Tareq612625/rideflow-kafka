"""Health and counters."""

from fastapi import APIRouter
from app import config
from app.producer import producer

router = APIRouter(tags=["system"])


@router.get("/stats")
async def stats():
    return producer.stats()


@router.post("/reset")
async def reset():
    producer.reset()
    return {"reset": True}


@router.get("/health")
async def health():
    return {
        "service": config.SERVICE_NAME,
        "role": "producer",
        "kafka": config.KAFKA_BOOTSTRAP_SERVERS,
        "topic": config.TOPIC,
        "produced": producer.sent,
    }
