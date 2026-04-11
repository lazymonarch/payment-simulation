import json

import redis.asyncio as aioredis
import structlog

from app.config import get_settings
from app.models import PaymentRequest

logger = structlog.get_logger(__name__)
settings = get_settings()


def _serialise_payload(payload: PaymentRequest) -> dict[str, str]:
    data = payload.model_dump(mode="python")
    serialised: dict[str, str] = {}

    for key, value in data.items():
        if value is None:
            continue
        if isinstance(value, (dict, list)):
            serialised[key] = json.dumps(value)
        else:
            serialised[key] = str(value)

    return serialised


async def publish_payment(
    payload: PaymentRequest,
    redis: aioredis.Redis,
) -> str:
    stream = settings.queue_stream_name
    fields = _serialise_payload(payload)

    try:
        message_id = await redis.xadd(stream, fields)
        logger.info(
            "queue.published",
            txn_id=payload.txn_id,
            merchant_id=payload.merchant_id,
            stream=stream,
            message_id=message_id,
        )
        return message_id
    except Exception as exc:
        logger.error("queue.publish_failed", txn_id=payload.txn_id, error=str(exc))
        raise


async def ensure_consumer_group(redis: aioredis.Redis) -> None:
    stream = settings.queue_stream_name
    group = settings.consumer_group

    try:
        await redis.xgroup_create(stream, group, id="$", mkstream=True)
        logger.info("queue.consumer_group_created", stream=stream, group=group)
    except Exception as exc:
        if "BUSYGROUP" in str(exc):
            logger.debug("queue.consumer_group_exists", stream=stream, group=group)
            return
        logger.error("queue.consumer_group_creation_failed", error=str(exc))
        raise
