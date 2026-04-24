from fastapi import APIRouter, HTTPException, Query

from app.api.dependencies import usage_repository
from app.api.response import ok
from app.schemas import ApiResponse
from app.services.usage_service import validate_ymd

router = APIRouter(prefix="/internal/staging", tags=["Internal"])


@router.get("/flexi", response_model=ApiResponse)
def get_staging_flexi(
    year: int | None = Query(None, ge=2020, le=2030),
    month: int | None = Query(None, ge=1, le=12),
    day: int | None = Query(None, ge=1, le=31),
    limit: int = Query(50, ge=1, le=1000000),
    offset: int = Query(0, ge=0),
    include_total: bool = Query(False),
):
    try:
        validate_ymd(year, month, day)
        rows, total = usage_repository.get_staging_flexi(
            year=year,
            month=month,
            day=day,
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
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(500, f"staging_flexi_error: {exc}") from exc


@router.get("/icc", response_model=ApiResponse)
def get_staging_icc(
    limit: int = Query(50, ge=1, le=1000000),
    offset: int = Query(0, ge=0),
):
    try:
        rows, total = usage_repository.get_staging_icc(limit=limit, offset=offset)
        return ok(rows, total=total, limit=limit, offset=offset)
    except Exception as exc:
        raise HTTPException(500, f"staging_icc_error: {exc}") from exc
