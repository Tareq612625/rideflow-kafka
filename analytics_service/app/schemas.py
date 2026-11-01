"""Request models."""

from pydantic import BaseModel, Field


class ThrottleIn(BaseModel):
    ms: float = Field(
        0, ge=0, le=50, description="artificial delay per message, to make lag visible"
    )
