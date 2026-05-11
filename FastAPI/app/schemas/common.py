from typing import Any

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: str = Field(examples=["ok"])
    database: str = Field(examples=["reachable"])


class ApiResponse(BaseModel):
    data: Any = None
    meta: dict[str, Any] = Field(default_factory=dict)
