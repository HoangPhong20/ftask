from fastapi import APIRouter

from .endpoints.staging import router as staging_router

internal_router = APIRouter()
internal_router.include_router(staging_router)
