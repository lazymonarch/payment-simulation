import json
from datetime import datetime
from decimal import Decimal
from typing import Optional

import redis.asyncio as aioredis
import structlog

from app.models import PaymentResponse

logger = structlog.get_logger(__name__)

IDEMPOTENCY_KEY_PREFIX = "idempotency"


def _make_key(txn_id: str) -> str:
    return f"{IDEMPOTENCY_KEY_PREFIX}:{txn_id}"


class DecimalDatetimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


def _serialise(response: PaymentResponse) -> str:
    return json.dumps(response.model_dump(mode="python"), cls=DecimalDatetimeEncoder)


def _deserialise(raw: str) -> PaymentResponse:
    data = json.loads(raw)
    return PaymentResponse(**data)


async def get_cached_response(
    txn_id: str,
    redis: aioredis.Redis,
) -> Optional[PaymentResponse]:
    key = _make_key(txn_id)

    try:
        raw = await redis.get(key)
    except Exception as exc:
        logger.error("idempotency.redis_read_failed", txn_id=txn_id, error=str(exc))
        return None

    if raw is None:
        logger.debug("idempotency.cache_miss", txn_id=txn_id)
        return None

    logger.info("idempotency.cache_hit", txn_id=txn_id)
    return _deserialise(raw)


async def store_response(
    txn_id: str,
    response: PaymentResponse,
    redis: aioredis.Redis,
    ttl_seconds: int = 86400,
) -> None:
    key = _make_key(txn_id)

    try:
        await redis.set(key, _serialise(response), ex=ttl_seconds)
        logger.info("idempotency.stored", txn_id=txn_id, ttl_seconds=ttl_seconds)
    except Exception as exc:
        logger.error("idempotency.redis_write_failed", txn_id=txn_id, error=str(exc))


async def is_duplicate(
    txn_id: str,
    redis: aioredis.Redis,
) -> bool:
    key = _make_key(txn_id)

    try:
        return bool(await redis.exists(key))
    except Exception:
        return False
