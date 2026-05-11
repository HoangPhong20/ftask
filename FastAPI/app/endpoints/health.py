from fastapi import APIRouter, Depends, HTTPException

from app.dependencies import get_usage_repository
from app.repositories.usage_repository import UsageRepository
from app.response import ok
from app.schemas import ApiResponse

router = APIRouter(tags=["System"])


@router.get("/health", response_model=ApiResponse)
def health_check(repo: UsageRepository = Depends(get_usage_repository)):
    if repo.health_check():
        return ok({"status": "ok", "database": "reachable"})
    raise HTTPException(503, "Database unavailable")
