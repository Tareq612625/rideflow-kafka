"""
RideFlow -- Analytics Service (Kafka CONSUMER)
==============================================
Reads every event from the "ride-events" topic, turns it into live statistics
and pushes them to the browser over a WebSocket. It also serves the dashboard.

    cd analytics_service
    uvicorn app.main:app --port 8001 --reload

Dashboard: http://localhost:8001
Docs:      http://localhost:8001/docs
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.consumer import ride_consumer
from app.routers import dashboard, messages, system
from app.ws import hub


@asynccontextmanager
async def lifespan(app: FastAPI):
    ride_consumer.start()
    hub.start()
    yield
    await ride_consumer.stop()
    await hub.stop()


app = FastAPI(
    title="RideFlow Analytics Service",
    description="Kafka consumer — live statistics and the dashboard.",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(dashboard.router)
app.include_router(messages.router)
app.include_router(system.router)
