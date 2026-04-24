from fastapi import FastAPI

from .api.router import api_router

# FastAPI app is just the bootstrap layer: create the app and mount routers.
app = FastAPI(
    title="Nifi Warehouse API",
    description="Read-only API for reporting system",
    version="1.0.0",
)

app.include_router(api_router)
