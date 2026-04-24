from datetime import date
from typing import Any

from fastapi import HTTPException
from sqlalchemy import Integer, MetaData, Table, and_, cast, desc, func, select
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError


class UsageRepository:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self._metadata = MetaData()
        self._table_cache: dict[tuple[str, str], Table] = {}

    def _table(self, conn: Connection, schema: str, table_name: str) -> Table:
        # Reflect tables lazily and reuse the metadata cache across calls.
        key = (schema, table_name)
        if key not in self._table_cache:
            self._table_cache[key] = Table(
                table_name,
                self._metadata,
                schema=schema,
                autoload_with=conn,
                extend_existing=True,
            )
        return self._table_cache[key]

    def _table_columns(self, conn: Connection, schema: str, table_name: str) -> set[str]:
        table = self._table(conn, schema, table_name)
        return {c.name for c in table.columns}

    @staticmethod
    def _rows(rows: Any) -> list[dict[str, Any]]:
        return [dict(r._mapping) for r in rows]

    @staticmethod
    def _require_columns_from_set(cols: set[str], required: list[str], label: str) -> None:
        missing = [c for c in required if c not in cols]
        if missing:
            raise HTTPException(409, f"missing required columns in {label}: {', '.join(missing)}")

    def health_check(self) -> bool:
        try:
            with self.engine.connect() as conn:
                conn.execute(select(1))
            return True
        except SQLAlchemyError:
            return False

    def get_staging_flexi(
        self,
        year: int | None,
        month: int | None,
        day: int | None,
        limit: int,
        offset: int,
        include_total: bool,
    ) -> tuple[list[dict[str, Any]], int | None]:
        with self.engine.connect() as conn:
            # Staging queries stay defensive because raw data can evolve without notice.
            table = self._table(conn, "public", "stg_frt_flexi_raw")
            cols = self._table_columns(conn, "public", "stg_frt_flexi_raw")

            conditions = []
            if year:
                required_cols = ["year"]
                if month:
                    required_cols.append("month")
                if day:
                    required_cols.append("day")
                self._require_columns_from_set(cols, required_cols, "public.stg_frt_flexi_raw")
                conditions.append(table.c.year == year)
                if month:
                    conditions.append(table.c.month == month)
                if day:
                    conditions.append(table.c.day == day)

            year_select = table.c.year if "year" in cols else cast(literal(None), Integer).label("year")
            month_select = table.c.month if "month" in cols else cast(literal(None), Integer).label("month")
            day_select = table.c.day if "day" in cols else cast(literal(None), Integer).label("day")

            stmt = (
                select(
                    table.c.charging_id,
                    table.c.record_sequence_number,
                    table.c.record_opening_time,
                    table.c.served_msisdn,
                    table.c.ftp_filename,
                    year_select,
                    month_select,
                    day_select,
                    table.c._source_file,
                    table.c._ingested_at,
                )
                .order_by(desc(table.c._ingested_at))
                .limit(limit)
                .offset(offset)
            )
            if conditions:
                stmt = stmt.where(and_(*conditions))

            data = self._rows(conn.execute(stmt).fetchall())

            total: int | None = None
            if include_total:
                count_stmt = select(func.count()).select_from(table)
                if conditions:
                    count_stmt = count_stmt.where(and_(*conditions))
                total = conn.execute(count_stmt).scalar()

        return data, total

    def get_staging_icc(self, limit: int, offset: int) -> tuple[list[dict[str, Any]], int]:
        with self.engine.connect() as conn:
            table = self._table(conn, "public", "stg_frt_in_icc_raw")
            stmt = (
                select(
                    table.c.org_call_id,
                    table.c.call_reference,
                    table.c.call_sta_time,
                    table.c.call_type,
                    table.c.used_duration,
                    table.c._source_file,
                    table.c._ingested_at,
                )
                .order_by(desc(table.c._ingested_at))
                .limit(limit)
                .offset(offset)
            )
            rows = self._rows(conn.execute(stmt).fetchall())
            total = conn.execute(select(func.count()).select_from(table)).scalar() or 0
        return rows, total

    def get_usage_daily(
        self,
        date_from: date,
        date_to: date,
        call_type: str | None,
        limit: int,
        offset: int,
        include_total: bool,
    ) -> tuple[list[dict[str, Any]], int | None]:
        with self.engine.connect() as conn:
            # Daily usage is the served shape for dashboards and reporting views.
            fact = self._table(conn, "dwh", "fact_usage_daily")
            dim = self._table(conn, "dwh", "dim_call_type")

            conditions = [
                fact.c.usage_date >= date_from,
                fact.c.usage_date <= date_to,
            ]
            if call_type:
                conditions.append(func.upper(func.trim(dim.c.call_type_code)) == call_type.upper())

            base = (
                select(
                    fact.c.usage_date,
                    dim.c.call_type_code,
                    fact.c.event_count,
                    fact.c.total_used_duration,
                )
                .select_from(fact.outerjoin(dim, dim.c.call_type_key == fact.c.call_type_key))
                .where(and_(*conditions))
            )
            stmt = base.order_by(desc(fact.c.usage_date), dim.c.call_type_code).limit(limit).offset(offset)
            rows = self._rows(conn.execute(stmt).fetchall())

            total: int | None = None
            if include_total:
                count_stmt = (
                    select(func.count())
                    .select_from(fact.outerjoin(dim, dim.c.call_type_key == fact.c.call_type_key))
                    .where(and_(*conditions))
                )
                total = conn.execute(count_stmt).scalar()
        return rows, total

    def get_usage_summary_range(
        self,
        date_from: date,
        date_to: date,
        call_type: str | None,
    ) -> list[dict[str, Any]]:
        with self.engine.connect() as conn:
            # Dashboard summary reads from the precomputed daily summary table, not the raw fact table.
            summary = self._table(conn, "dwh", "usage_summary_daily")

            conditions = [
                summary.c.usage_date >= date_from,
                summary.c.usage_date <= date_to,
            ]
            if call_type:
                conditions.append(func.upper(func.trim(summary.c.call_type_code)) == call_type.upper())

            event_count_expr = cast(func.sum(func.coalesce(summary.c.event_count, 0)), Integer)
            duration_expr = cast(
                func.sum(func.coalesce(summary.c.total_used_duration, 0)),
                summary.c.total_used_duration.type,
            )
            stmt = (
                select(
                    summary.c.call_type_code,
                    event_count_expr.label("event_count"),
                    duration_expr.label("total_used_duration"),
                )
                .select_from(summary)
                .where(and_(*conditions))
                .group_by(summary.c.call_type_code)
                .order_by(desc(event_count_expr), summary.c.call_type_code)
            )
            rows = self._rows(conn.execute(stmt).fetchall())
        return rows

    def get_usage_trend_range(
        self,
        date_from: date,
        date_to: date,
        grain: str,
        call_type: str | None,
    ) -> list[dict[str, Any]]:
        with self.engine.connect() as conn:
            # Trend reads the precomputed daily summary table and re-aggregates only the smaller data set.
            summary = self._table(conn, "dwh", "usage_summary_daily")

            conditions = [
                summary.c.usage_date >= date_from,
                summary.c.usage_date <= date_to,
            ]
            if call_type:
                conditions.append(func.upper(func.trim(summary.c.call_type_code)) == call_type.upper())

            if grain == "day":
                period_expr = func.to_char(summary.c.usage_date, "YYYY-MM-DD")
            else:
                period_expr = func.to_char(func.date_trunc("month", summary.c.usage_date), "YYYY-MM")

            event_count_expr = cast(func.sum(func.coalesce(summary.c.event_count, 0)), Integer)
            duration_expr = cast(
                func.sum(func.coalesce(summary.c.total_used_duration, 0)),
                summary.c.total_used_duration.type,
            )
            stmt = (
                select(
                    period_expr.label("period"),
                    event_count_expr.label("event_count"),
                    duration_expr.label("total_used_duration"),
                )
                .select_from(summary)
                .where(and_(*conditions))
                .group_by(period_expr)
                .order_by(period_expr)
            )
            rows = self._rows(conn.execute(stmt).fetchall())
        return rows
