import redis.asyncio as aioredis

from app.config import get_settings
from app.observability.logger import get_logger

logger = get_logger(__name__)


async def create_redis() -> aioredis.Redis:
    settings = get_settings()
    client = aioredis.from_url(
        settings.redis_url,
        encoding="utf-8",
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=5,
        retry_on_timeout=True,
    )
    logger.info("redis.client_created", url=settings.redis_url)
    return client


async def close_redis(client: aioredis.Redis) -> None:
    await client.aclose()
    logger.info("redis.client_closed")


async def check_connection(client: aioredis.Redis) -> bool:
    try:
        await client.ping()
        return True
    except Exception as exc:
        logger.error("redis.connection_check_failed", error=str(exc))
        return False
