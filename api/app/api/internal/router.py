from fastapi import APIRouter, Depends

from app.api.dependencies import verify_api_key

from .endpoints.staging import router as staging_router

internal_router = APIRouter()
internal_router.include_router(staging_router, dependencies=[Depends(verify_api_key)])
