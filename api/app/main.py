from fastapi import FastAPI

from .api.router import api_router
from .core.database import wait_for_database

# FastAPI app is just the bootstrap layer: create the app and mount routers.
app = FastAPI(
    title="Nifi Warehouse API",
    description="Read-only API for reporting system",
    version="1.0.0",
)


@app.on_event("startup")
def _startup_wait_for_db() -> None:
    wait_for_database()


app.include_router(api_router)
