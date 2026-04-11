"""
Per-merchant request limiting before enqueue.

Each wall-clock second is a bucket (Unix second in the Redis key). Atomic INCR
plus a one-shot EXPIRE on first touch gives correct counts under concurrency
without Lua. TTL is 2s so keys expire shortly after the bucket ends (buffer for
boundary timing).
"""

import time

import redis.asyncio as aioredis
import structlog

from app.config import get_settings

logger = structlog.get_logger(__name__)
settings = get_settings()

RATE_LIMIT_KEY_PREFIX = "ratelimit"

# Per-merchant overrides — if a merchant_id is in this dict,
# their limit overrides the default from settings.
# In production this would be read from a database or config service.
MERCHANT_LIMITS: dict[str, int] = {
    "merchant_test":     10,    # Low limit for easy testing
    "merchant_premium":  500,   # Premium tier
}


def _make_key(merchant_id: str, window: int) -> str:
    """
    Build the Redis key for a merchant's current time window.
    window = current Unix timestamp in seconds.
    Example: ratelimit:merchant_flipkart:1712345678
    """
    return f"{RATE_LIMIT_KEY_PREFIX}:{merchant_id}:{window}"


def _get_limit(merchant_id: str) -> int:
    """
    Return the rate limit for a given merchant.
    Checks the override table first, falls back to the default.
    """
    return MERCHANT_LIMITS.get(merchant_id, settings.default_rate_limit_per_sec)


async def check_rate_limit(
    merchant_id: str,
    redis: aioredis.Redis,
) -> tuple[bool, int, int]:
    """
    Check whether the merchant is within their rate limit for the current second.

    Returns:
        allowed  (bool) — True if the request should proceed
        current  (int)  — how many requests this merchant has made this second
        limit    (int)  — their allowed limit per second

    Uses atomic INCR + EXPIRE — no race conditions under concurrent load.
    """
    limit   = _get_limit(merchant_id)
    window  = int(time.time())
    key     = _make_key(merchant_id, window)

    try:
        # Atomic increment — returns the new count after incrementing
        current = await redis.incr(key)

        if current == 1:
            # First request in this window — set the expiry
            # TTL of 2 seconds ensures the key outlives the window by 1s
            # to handle clock skew between requests at window boundaries
            await redis.expire(key, 2)

        allowed = current <= limit

        if not allowed:
            logger.warning(
                "rate_limit.exceeded",
                merchant_id=merchant_id,
                current=current,
                limit=limit,
                window=window,
            )
        else:
            logger.debug(
                "rate_limit.allowed",
                merchant_id=merchant_id,
                current=current,
                limit=limit,
            )

        return allowed, current, limit

    except Exception as e:
        # Redis is unavailable — fail open (allow the request through)
        # Failing closed would block all payments when Redis hiccups.
        # The queue and idempotency layer are the next line of defense.
        logger.error(
            "rate_limit.redis_error",
            merchant_id=merchant_id,
            error=str(e),
        )
        return True, 0, limit


async def log_rate_limit_event(
    merchant_id: str,
    current_rate: float,
    db_pool,
) -> None:
    """
    Persist a rate limit violation to Postgres for the ops team.
    Called asynchronously after returning 429 — never blocks the response.
    """
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO rate_limit_events (merchant_id, request_rate)
                VALUES ($1, $2)
                """,
                merchant_id,
                current_rate,
            )
    except Exception as e:
        logger.error(
            "rate_limit.log_event_failed",
            merchant_id=merchant_id,
            error=str(e),
        )