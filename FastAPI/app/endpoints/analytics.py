import logging
from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query

from app.dependencies import get_analytics_service
from app.response import ok
from app.schemas import ApiResponse
from app.services.analytics_service import AnalyticsService

router = APIRouter(prefix="/api/analytics", tags=["Analytics"])
logger = logging.getLogger(__name__)


@router.get("/metrics/usage-summary", response_model=ApiResponse)
def get_usage_summary(
    from_date: date = Query(..., alias="from"),
    to_date: date = Query(..., alias="to"),
    call_type: str | None = None,
    service: AnalyticsService = Depends(get_analytics_service),
):
    if to_date < from_date:
        raise HTTPException(400, "to must be greater than or equal to from")
    try:
        rows, meta = service.summary(
            date_from=from_date,
            date_to=to_date,
            call_type=call_type,
        )
        return ok(rows, **meta)
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Failed to fetch usage summary")
        raise HTTPException(500, "Internal server error") from exc


@router.get("/metrics/usage-trend", response_model=ApiResponse)
def get_usage_trend(
    from_date: date = Query(..., alias="from"),
    to_date: date = Query(..., alias="to"),
    grain: str = Query("day"),
    call_type: str | None = None,
    service: AnalyticsService = Depends(get_analytics_service),
):
    if to_date < from_date:
        raise HTTPException(400, "to must be greater than or equal to from")
    try:
        rows, meta = service.trend(
            date_from=from_date,
            date_to=to_date,
            grain=grain,
            call_type=call_type,
        )
        return ok(rows, **meta)
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Failed to fetch usage trend")
        raise HTTPException(500, "Internal server error") from exc
