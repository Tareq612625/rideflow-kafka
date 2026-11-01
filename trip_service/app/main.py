"""
RideFlow -- Trip Service (Kafka PRODUCER)
=========================================
A ride-hailing app. Every time something happens to a trip it publishes one
event to the Kafka topic "ride-events". It has no idea who reads them.

    cd trip_service
    uvicorn app.main:app --port 8000 --reload

Docs: http://localhost:8000/docs
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app import config
from app.producer import producer
from app.routers import system, trips


@asynccontextmanager
async def lifespan(app: FastAPI):
    await producer.start()
    yield
    await producer.stop()


app = FastAPI(
    title="RideFlow Trip Service",
    description="Kafka producer — publishes ride events.",
    version="1.0.0",
    lifespan=lifespan,
)

# the dashboard is served by the analytics service on another port
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

app.include_router(trips.router)
app.include_router(system.router)


@app.get("/", include_in_schema=False)
async def root():
    return {
        "service": config.SERVICE_NAME,
        "role": "kafka producer",
        "topic": config.TOPIC,
        "docs": "/docs",
        "dashboard": "http://localhost:8001",
    }
