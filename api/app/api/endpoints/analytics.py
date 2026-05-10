from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query

from app.api.dependencies import get_analytics_service
from app.api.response import ok
from app.schemas import ApiResponse
from app.services.analytics_service import AnalyticsService

router = APIRouter(prefix="/api/analytics", tags=["Analytics"])


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
        raise HTTPException(500, f"analytics_usage_summary_error: {exc}") from exc


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
        raise HTTPException(500, f"analytics_usage_trend_error: {exc}") from exc
