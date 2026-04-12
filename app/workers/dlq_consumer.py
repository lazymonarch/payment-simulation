import asyncio
import json
from datetime import datetime

import asyncpg
import redis.asyncio as aioredis
import structlog

from app.config import get_settings

logger = structlog.get_logger(__name__)
settings = get_settings()

DLQ_CONSUMER_GROUP = "dlq-processors"
DLQ_CONSUMER_NAME = "dlq-worker-0"


async def _ensure_dlq_group(redis: aioredis.Redis) -> None:
    try:
        await redis.xgroup_create(
            settings.dlq_stream_name,
            DLQ_CONSUMER_GROUP,
            id="$",
            mkstream=True,
        )
        logger.info("dlq.consumer_group_created", stream=settings.dlq_stream_name)
    except Exception as exc:
        if "BUSYGROUP" in str(exc):
            logger.debug("dlq.consumer_group_exists")
        else:
            raise


def _parse_attempt_times(raw: str | None) -> list[datetime]:
    if not raw:
        return []
    try:
        values = json.loads(raw)
        return [datetime.fromisoformat(value) for value in values]
    except Exception:
        return []


async def _persist_to_postgres(fields: dict, db_pool: asyncpg.Pool) -> None:
    failed_at = fields.get("failed_at")
    attempt_times = _parse_attempt_times(fields.get("all_attempt_times"))
    if failed_at:
        try:
            attempt_times.append(datetime.fromisoformat(failed_at))
        except Exception:
            pass

    original_payload = {
        "txn_id": fields.get("txn_id"),
        "merchant_id": fields.get("merchant_id"),
        "amount": fields.get("amount"),
        "currency": fields.get("currency"),
        "payment_method": fields.get("payment_method"),
        "metadata": fields.get("original_metadata"),
    }

    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO dlq_transactions (
                txn_id,
                merchant_id,
                original_payload,
                failure_reason,
                retry_count,
                all_attempt_times,
                error_trace,
                created_at
            ) VALUES ($1, $2, $3::jsonb, $4, $5, $6, $7, NOW())
            ON CONFLICT (txn_id) DO NOTHING
            """,
            fields.get("txn_id"),
            fields.get("merchant_id"),
            json.dumps(original_payload),
            fields.get("failure_reason"),
            int(fields.get("retry_count", "0")),
            attempt_times,
            fields.get("error_trace") or None,
        )

    logger.warning(
        "dlq.persisted_to_postgres",
        txn_id=fields.get("txn_id"),
        failure_reason=fields.get("failure_reason"),
        retry_count=fields.get("retry_count"),
    )


async def run_dlq_consumer(
    db_pool: asyncpg.Pool,
    redis: aioredis.Redis,
) -> None:
    await _ensure_dlq_group(redis)

    logger.info("dlq.consumer_started", stream=settings.dlq_stream_name)

    while True:
        try:
            results = await redis.xreadgroup(
                groupname=DLQ_CONSUMER_GROUP,
                consumername=DLQ_CONSUMER_NAME,
                streams={settings.dlq_stream_name: ">"},
                count=5,
                block=3000,
            )

            if not results:
                continue

            for _, messages in results:
                for message_id, fields in messages:
                    try:
                        await _persist_to_postgres(fields, db_pool)
                        await redis.xack(
                            settings.dlq_stream_name,
                            DLQ_CONSUMER_GROUP,
                            message_id,
                        )
                    except Exception as exc:
                        logger.error(
                            "dlq.persist_failed",
                            message_id=message_id,
                            error=str(exc),
                        )
        except asyncio.CancelledError:
            logger.info("dlq.consumer_cancelled")
            break
        except Exception as e:
            error_str = str(e)

            if "NOGROUP" in error_str:
                logger.warning("dlq.nogroup_detected", error=error_str)
                try:
                    await _ensure_dlq_group(redis)
                    logger.info("dlq.consumer_group_recreated")
                except Exception as recreate_err:
                    logger.error(
                        "dlq.consumer_group_recreate_failed",
                        error=str(recreate_err),
                    )
                    await asyncio.sleep(2)
            else:
                logger.error("dlq.consumer_loop_error", error=error_str)
                await asyncio.sleep(2)
