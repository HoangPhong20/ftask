from fastapi import APIRouter, HTTPException, Query

from app.api.dependencies import usage_service
from app.api.response import ok
from app.schemas import ApiResponse

router = APIRouter(prefix="/api/usage", tags=["Usage"])


@router.get("/daily", response_model=ApiResponse)
def get_usage_daily(
    year: int = Query(..., ge=2020, le=2030),
    month: int | None = Query(None, ge=1, le=12),
    day: int | None = Query(None, ge=1, le=31),
    call_type: str | None = None,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    include_total: bool = Query(False),
):
    try:
        rows, total, date_from, date_to = usage_service.daily(
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
        raise HTTPException(500, f"usage_daily_error: {exc}") from exc
