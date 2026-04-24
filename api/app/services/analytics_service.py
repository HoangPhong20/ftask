import json
from datetime import date
from decimal import Decimal
from typing import Any

from fastapi import HTTPException

from ..core.cache import TTLCache
from ..repositories.usage_repository import UsageRepository


class AnalyticsService:
    def __init__(self, repo: UsageRepository, cache: TTLCache | None = None) -> None:
        self.repo = repo
        self.cache = cache

    def _cache_key(self, payload: dict[str, Any]) -> str:
        return json.dumps(payload, sort_keys=True, default=str)

    def summary(
        self,
        date_from: date,
        date_to: date,
        call_type: str | None,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        # Summary endpoints are cached because dashboards usually hit the same time range repeatedly.
        key = self._cache_key(
            {
                "endpoint": "analytics-summary",
                "date_from": date_from,
                "date_to": date_to,
                "call_type": call_type,
            }
        )
        cached = self.cache.get(key) if self.cache else None
        if cached is not None:
            return cached["data"], cached["meta"]

        rows = self.repo.get_usage_summary_range(
            date_from=date_from,
            date_to=date_to,
            call_type=call_type,
        )
        meta = {
            "date_from": date_from,
            "date_to": date_to,
            "call_type": call_type,
            "totals": {
                "event_count": int(sum((r.get("event_count") or 0) for r in rows)),
                "total_used_duration": sum((r.get("total_used_duration") or Decimal(0)) for r in rows),
            },
        }
        if self.cache:
            self.cache.set(key, {"data": rows, "meta": meta})
        return rows, meta

    def trend(
        self,
        date_from: date,
        date_to: date,
        grain: str,
        call_type: str | None,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        if grain not in {"day", "month"}:
            raise HTTPException(400, "grain must be one of: day, month")

        # Trend uses the same cache strategy as summary, but keeps grain in the key.
        key = self._cache_key(
            {
                "endpoint": "analytics-trend",
                "date_from": date_from,
                "date_to": date_to,
                "grain": grain,
                "call_type": call_type,
            }
        )
        cached = self.cache.get(key) if self.cache else None
        if cached is not None:
            return cached["data"], cached["meta"]

        rows = self.repo.get_usage_trend_range(
            date_from=date_from,
            date_to=date_to,
            grain=grain,
            call_type=call_type,
        )
        meta = {
            "date_from": date_from,
            "date_to": date_to,
            "grain": grain,
            "call_type": call_type,
        }
        if self.cache:
            self.cache.set(key, {"data": rows, "meta": meta})
        return rows, meta
