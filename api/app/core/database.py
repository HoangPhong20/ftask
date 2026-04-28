import logging
import os
import time

from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text

from .config import get_settings

settings = get_settings()
logger = logging.getLogger("api.database")

engine = create_engine(
    settings.database_url,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
)


def wait_for_database() -> None:
    max_retries = max(int(os.getenv("DB_CONNECT_MAX_RETRIES", "30")), 1)
    sleep_seconds = max(float(os.getenv("DB_CONNECT_RETRY_SLEEP_SECONDS", "2")), 0.0)

    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database connection ready on attempt %s/%s", attempt, max_retries)
            return
        except SQLAlchemyError as exc:
            last_error = exc
            logger.warning(
                "Database not ready yet (attempt %s/%s): %s",
                attempt,
                max_retries,
                exc,
            )
            if attempt < max_retries:
                time.sleep(sleep_seconds)

    assert last_error is not None
    raise last_error
