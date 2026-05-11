import logging
import time

from sqlalchemy import text
from sqlalchemy import create_engine
from sqlalchemy.exc import DatabaseError, InterfaceError, OperationalError, SQLAlchemyError

from .config import get_settings

settings = get_settings()
logger = logging.getLogger("api.database")

engine = create_engine(
    settings.database_url,
    pool_pre_ping=True,
    pool_size=settings.database_pool_size,
    max_overflow=settings.database_max_overflow,
    pool_timeout=settings.database_pool_timeout_seconds,
    pool_recycle=settings.database_pool_recycle_seconds,
)


def _database_error_kind(exc: SQLAlchemyError) -> str:
    if isinstance(exc, OperationalError):
        return "operational"
    if isinstance(exc, InterfaceError):
        return "interface"
    if isinstance(exc, DatabaseError):
        return "database"
    return "sqlalchemy"


def wait_for_database() -> None:
    max_retries = max(settings.database_connect_max_retries, 1)
    sleep_seconds = max(settings.database_connect_retry_initial_seconds, 0.0)
    max_sleep_seconds = max(settings.database_connect_retry_max_seconds, sleep_seconds)

    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database connection ready on attempt %s/%s", attempt, max_retries)
            return
        except SQLAlchemyError as exc:
            last_error = exc
            error_kind = _database_error_kind(exc)
            logger.warning(
                "Database connection failed (kind=%s, attempt=%s/%s): %s",
                error_kind,
                attempt,
                max_retries,
                exc,
            )
            if attempt < max_retries:
                time.sleep(sleep_seconds)
                sleep_seconds = min(sleep_seconds * 2, max_sleep_seconds)

    raise RuntimeError("Database connection failed") from last_error
