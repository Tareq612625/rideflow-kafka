"""
RideFlow -- Billing Service  (Kafka CONSUMER *and* PRODUCER)
============================================================
Reads the SAME "ride-events" topic as the analytics service, but under its own
consumer group — so both services receive every message, independently.

It ignores everything except completed trips, turns those into invoices, and
publishes them to a second topic:

    ride-events  ──read──►  billing-service  ──write──►  invoices

    cd billing_service
    uvicorn app.main:app --port 8002 --reload

Page: http://localhost:8002
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.consumer import billing_consumer
from app.routers import dashboard, system
from app.ws import hub


@asynccontextmanager
async def lifespan(app: FastAPI):
    billing_consumer.start()
    hub.start()
    yield
    await billing_consumer.stop()
    await hub.stop()


app = FastAPI(
    title="RideFlow Billing Service",
    description="Kafka consumer + producer — turns completed trips into invoices.",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(dashboard.router)
app.include_router(system.router)
