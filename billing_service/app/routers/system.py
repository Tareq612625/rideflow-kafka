"""Stats and health."""

from fastapi import APIRouter
from app import config
from app.state import billing

router = APIRouter(tags=["system"])


@router.get("/stats")
async def stats():
    return billing.snapshot()


@router.post("/reset")
async def reset():
    billing.reset()
    return {"reset": True}


@router.get("/health")
async def health():
    return {
        "service": config.SERVICE_NAME,
        "role": "consumer + producer",
        "kafka": config.KAFKA_BOOTSTRAP_SERVERS,
        "reads": config.SOURCE_TOPIC,
        "writes": config.INVOICE_TOPIC,
        "group_id": config.GROUP_ID,
        "connected": billing.connected,
        "invoices": billing.invoices,
    }
