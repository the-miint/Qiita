"""Shared Pydantic models: work ticket states, API schemas, identifier types."""

from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str
    service: str
