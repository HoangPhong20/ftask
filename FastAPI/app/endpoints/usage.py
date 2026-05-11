import logging

from fastapi import APIRouter, Depends, HTTPException, Query

from app.dependencies import get_usage_service
from app.response import ok
from app.schemas import ApiResponse
from app.services.usage_service import UsageService

router = APIRouter(prefix="/api/usage", tags=["Usage"])
logger = logging.getLogger(__name__)


@router.get("/daily", response_model=ApiResponse)
def get_usage_daily(
    year: int = Query(..., ge=2020, le=2030),
    month: int | None = Query(None, ge=1, le=12),
    day: int | None = Query(None, ge=1, le=31),
    call_type: str | None = None,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    include_total: bool = Query(False),
    service: UsageService = Depends(get_usage_service),
):
    try:
        rows, total, date_from, date_to = service.daily(
            year=year,
            month=month,
            day=day,
            call_type=call_type,
            limit=limit,
            offset=offset,
            include_total=include_total,
        )
        return ok(
            rows,
            total=total,
            limit=limit,
            offset=offset,
            year=year,
            month=month,
            day=day,
            call_type=call_type,
            date_from=date_from,
            date_to=date_to,
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Failed to fetch daily usage")
        raise HTTPException(500, "Internal server error") from exc
