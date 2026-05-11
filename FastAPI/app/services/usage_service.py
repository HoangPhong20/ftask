import calendar
import json
from datetime import date

from fastapi import HTTPException

from ..core.cache import TTLCache
from ..repositories.usage_repository import UsageRepository


def validate_ymd(year: int | None, month: int | None, day: int | None) -> None:
    if month and not year:
        raise HTTPException(400, "month requires year")
    if day and (not year or not month):
        raise HTTPException(400, "day requires year and month")
    if year is not None and month is not None and day is not None:
        last_day = calendar.monthrange(year, month)[1]
        if day > last_day:
            raise HTTPException(400, f"invalid day for month: {month}")


def date_range_from_ymd(year: int, month: int | None, day: int | None) -> tuple[date, date]:
    if day is not None and month is None:
        raise HTTPException(400, "day requires month")

    if month is None:
        return date(year, 1, 1), date(year, 12, 31)

    last_day = calendar.monthrange(year, month)[1]
    if day is None:
        return date(year, month, 1), date(year, month, last_day)

    if day > last_day:
        raise HTTPException(400, f"invalid day for month: {month}")
    return date(year, month, day), date(year, month, day)


class UsageService:
    def __init__(self, repo: UsageRepository, cache: TTLCache | None = None) -> None:
        self.repo = repo
        self.cache = cache

    def _cache_key(self, payload: dict) -> str:
        return json.dumps(payload, sort_keys=True, default=str)

    def daily(
        self,
        year: int,
        month: int | None,
        day: int | None,
        call_type: str | None,
        limit: int,
        offset: int,
        include_total: bool,
    ) -> tuple[list[dict], int | None, date, date]:
        # Keep this service focused on paging and date normalization for daily usage data.
        validate_ymd(year, month, day)
        date_from, date_to = date_range_from_ymd(year, month, day)

        key = self._cache_key(
            {
                "endpoint": "usage-daily",
                "year": year,
                "month": month,
                "day": day,
                "call_type": call_type,
                "limit": limit,
                "offset": offset,
                "include_total": include_total,
            }
        )
        cached = self.cache.get(key) if self.cache else None
        if cached is not None:
            return cached["rows"], cached["total"], cached["date_from"], cached["date_to"]

        rows, total = self.repo.get_usage_daily(
            date_from=date_from,
            date_to=date_to,
            call_type=call_type,
            limit=limit,
            offset=offset,
            include_total=include_total,
        )
        if self.cache:
            self.cache.set(
                key,
                {
                    "rows": rows,
                    "total": total,
                    "date_from": date_from,
                    "date_to": date_to,
                },
            )
        return rows, total, date_from, date_to
