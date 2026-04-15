from datetime import date

from fastapi import FastAPI, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from .database import engine
from .schemas import HealthResponse, UsageDailyOut

app = FastAPI(
    title="Nifi Warehouse API",
    description=(
        "Read-only API cho he thong bao cao web/app doc du lieu tu PostgreSQL. "
        "Tat ca endpoint duoi day chi dung de truy van va hien thi bao cao."
    ),
    version="1.0.0",
    openapi_tags=[
        {"name": "System", "description": "Trang thai he thong"},
        {"name": "Staging", "description": "Doc du lieu staging (raw)"},
        {"name": "Reporting", "description": "Doc du lieu tong hop phuc vu bao cao"},
    ],
)


def _row_to_dict(row):
    return dict(row._mapping)


@app.get(
    "/health",
    response_model=HealthResponse,
    tags=["System"],
    summary="Health Check",
    description="Kiem tra API co ket noi duoc toi PostgreSQL hay khong.",
)
def health_check():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "ok", "database": "reachable"}
    except SQLAlchemyError as exc:
        raise HTTPException(status_code=503, detail=f"Database unavailable: {exc}") from exc


@app.get(
    "/api/v1/staging/flexi",
    tags=["Staging"],
    summary="Lay Du Lieu Staging Flexi",
    description="Lay danh sach ban ghi moi nhat tu bang public.stg_frt_flexi_raw.",
)
def get_staging_flexi(
    limit: int = Query(
        default=100,
        ge=1,
        le=1000,
        description="So luong ban ghi toi da tra ve.",
        example=100,
    )
):
    try:
        sql = text(
            """
            SELECT charging_id, record_sequence_number, record_opening_time,
                   served_msisdn, ftp_filename, _source_file, _ingested_at
            FROM public.stg_frt_flexi_raw
            ORDER BY _ingested_at DESC NULLS LAST
            LIMIT :limit
            """
        )
        with engine.connect() as conn:
            rows = conn.execute(sql, {"limit": limit}).fetchall()
        return [_row_to_dict(r) for r in rows]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"staging_flexi_error: {exc}") from exc


@app.get(
    "/api/v1/staging/icc",
    tags=["Staging"],
    summary="Lay Du Lieu Staging ICC",
    description="Lay danh sach ban ghi moi nhat tu bang public.stg_frt_in_icc_raw.",
)
def get_staging_icc(
    limit: int = Query(
        default=100,
        ge=1,
        le=1000,
        description="So luong ban ghi toi da tra ve.",
        example=100,
    )
):
    try:
        sql = text(
            """
            SELECT org_call_id, call_reference, call_sta_time, call_type,
                   used_duration, _source_file, _ingested_at
            FROM public.stg_frt_in_icc_raw
            ORDER BY _ingested_at DESC NULLS LAST
            LIMIT :limit
            """
        )
        with engine.connect() as conn:
            rows = conn.execute(sql, {"limit": limit}).fetchall()
        return [_row_to_dict(r) for r in rows]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"staging_icc_error: {exc}") from exc


@app.get(
    "/api/v1/usage/daily",
    response_model=list[UsageDailyOut],
    tags=["Reporting"],
    summary="Lay Bao Cao Usage Theo Ngay",
    description="Truy van du lieu tong hop usage theo ngay, co filter ngay va loai cuoc goi.",
)
def get_usage_daily(
    date_from: date | None = Query(
        default=None,
        description="Ngay bat dau loc du lieu (dinh dang YYYY-MM-DD).",
        example="2026-04-01",
    ),
    date_to: date | None = Query(
        default=None,
        description="Ngay ket thuc loc du lieu (dinh dang YYYY-MM-DD).",
        example="2026-04-15",
    ),
    call_type_code: str | None = Query(
        default=None,
        description="Ma loai cuoc goi, vi du VOICE, SMS, DATA.",
        example="VOICE",
    ),
    limit: int = Query(
        default=100,
        ge=1,
        le=2000,
        description="So luong ban ghi toi da tra ve.",
        example=100,
    ),
):
    conditions = []
    params = {"limit": limit}

    if date_from is not None:
        conditions.append("f.usage_date >= :date_from")
        params["date_from"] = date_from
    if date_to is not None:
        conditions.append("f.usage_date <= :date_to")
        params["date_to"] = date_to
    if call_type_code:
        conditions.append("upper(trim(f.call_type)) = :call_type_code")
        params["call_type_code"] = call_type_code.strip().upper()

    where_clause = ""
    if conditions:
        where_clause = "WHERE " + " AND ".join(conditions)

    sql = text(
        f"""
        SELECT
            f.usage_date,
            f.call_type AS call_type_code,
            f.event_count,
            f.total_used_duration
        FROM dwh.fact_usage_daily f
        {where_clause}
        ORDER BY f.usage_date DESC NULLS LAST, f.call_type
        LIMIT :limit
        """
    )

    with engine.connect() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [_row_to_dict(r) for r in rows]
