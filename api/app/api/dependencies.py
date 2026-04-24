from ..core.cache import TTLCache
from ..core.config import get_settings
from ..core.database import engine
from ..repositories.usage_repository import UsageRepository
from ..services.analytics_service import AnalyticsService
from ..services.usage_service import UsageService

settings = get_settings()

# Central factory helpers make it easy to swap wiring later (DB/session/cache).
def get_db():
    with engine.connect() as conn:
        yield conn


def get_usage_repository() -> UsageRepository:
    return UsageRepository(engine)


def get_usage_service() -> UsageService:
    return UsageService(
        repo=get_usage_repository(),
        cache=TTLCache(
            ttl_seconds=settings.dashboard_cache_ttl_seconds,
            max_entries=settings.dashboard_cache_max_entries,
        ),
    )


def get_analytics_service() -> AnalyticsService:
    return AnalyticsService(
        repo=get_usage_repository(),
        cache=TTLCache(
            ttl_seconds=settings.dashboard_cache_ttl_seconds,
            max_entries=settings.dashboard_cache_max_entries,
        ),
    )


usage_repository = get_usage_repository()
usage_service = get_usage_service()
analytics_service = get_analytics_service()
