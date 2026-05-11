from functools import lru_cache

from fastapi import HTTPException, Security
from fastapi.security import APIKeyHeader

from .core.cache import TTLCache
from .core.config import get_settings
from .core.database import engine
from .repositories.usage_repository import UsageRepository
from .services.analytics_service import AnalyticsService
from .services.usage_service import UsageService

settings = get_settings()
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def verify_api_key(key: str | None = Security(api_key_header)) -> None:
    if not settings.internal_api_key or key != settings.internal_api_key:
        raise HTTPException(status_code=403, detail="Invalid API key")


# Central factory helpers make it easy to swap wiring later (DB/session/cache).
def get_db():
    with engine.connect() as conn:
        yield conn


@lru_cache(maxsize=1)
def get_usage_repository() -> UsageRepository:
    return UsageRepository(engine)


@lru_cache(maxsize=1)
def get_usage_cache() -> TTLCache:
    return TTLCache(
        ttl_seconds=settings.dashboard_cache_ttl_seconds,
        max_entries=settings.dashboard_cache_max_entries,
    )


@lru_cache(maxsize=1)
def get_analytics_cache() -> TTLCache:
    return TTLCache(
        ttl_seconds=settings.dashboard_cache_ttl_seconds,
        max_entries=settings.dashboard_cache_max_entries,
    )


@lru_cache(maxsize=1)
def get_usage_service() -> UsageService:
    return UsageService(
        repo=get_usage_repository(),
        cache=get_usage_cache(),
    )


@lru_cache(maxsize=1)
def get_analytics_service() -> AnalyticsService:
    return AnalyticsService(
        repo=get_usage_repository(),
        cache=get_analytics_cache(),
    )
