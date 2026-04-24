from fastapi import APIRouter, HTTPException

from app.api.dependencies import usage_repository
from app.api.response import ok
from app.schemas import ApiResponse

router = APIRouter(tags=["System"])


@router.get("/health", response_model=ApiResponse)
def health_check():
    if usage_repository.health_check():
        return ok({"status": "ok", "database": "reachable"})
    raise HTTPException(503, "Database unavailable")
