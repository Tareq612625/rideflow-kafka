"""Request/response models."""

from typing import Literal
from pydantic import BaseModel, Field


class TripIn(BaseModel):
    """Optional overrides when publishing a single event by hand."""

    driver_id: str | None = Field(None, pattern=r"^[A-Za-z0-9-]{1,32}$")
    zone: str | None = Field(None, pattern=r"^[A-Za-z ]{1,32}$")
    event_type: Literal["requested", "accepted", "completed", "cancelled"] | None = None


class BrokenIn(BaseModel):
    kind: Literal["missing-fields", "not-json"] = "missing-fields"


class SimulateIn(BaseModel):
    count: int = Field(1000, ge=1, le=200_000, description="how many events to produce")
    rate: int = Field(0, ge=0, le=100_000, description="messages per second, 0 = flat out")
    drivers: int = Field(250, ge=1, le=10_000, description="size of the driver pool")


class StoredIn(BaseModel):
    topic: str
    partition: int
    offset: int


class TripOut(BaseModel):
    stored_in: StoredIn
    event: dict
