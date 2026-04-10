import asyncpg

from app.config import get_settings
from app.observability.logger import get_logger

logger = get_logger(__name__)


async def create_pool() -> asyncpg.Pool:
    settings = get_settings()
    pool = await asyncpg.create_pool(
        dsn=settings.database_url,
        min_size=2,
        max_size=10,
        command_timeout=30,
    )
    logger.info("database.pool_created", min_size=2, max_size=10)
    return pool


async def close_pool(pool: asyncpg.Pool) -> None:
    await pool.close()
    logger.info("database.pool_closed")


async def check_connection(pool: asyncpg.Pool) -> bool:
    try:
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return True
    except Exception as exc:
        logger.error("database.connection_check_failed", error=str(exc))
        return False
