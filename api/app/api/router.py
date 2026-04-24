from fastapi import APIRouter

from .endpoints.analytics import router as analytics_router
from .endpoints.health import router as health_router
from .endpoints.usage import router as usage_router
from .internal.router import internal_router

# Keep the top-level router thin so feature routers can evolve independently.
api_router = APIRouter()
api_router.include_router(health_router)
api_router.include_router(usage_router)
api_router.include_router(analytics_router)
api_router.include_router(internal_router)
