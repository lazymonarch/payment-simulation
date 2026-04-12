import asyncio
import json
from decimal import Decimal

import asyncpg
import redis.asyncio as aioredis
import structlog

from app.config import get_settings
from app.models import PaymentRequest, TransactionStatus
from app.observability.metrics import active_workers, dlq_depth, queue_depth
from app.services.idempotency import store_response
from app.services.payment_processor import process_transaction
from app.services.queue_publisher import ensure_consumer_group
from app.services.retry_engine import handle_failed_transaction

logger = structlog.get_logger(__name__)
settings = get_settings()


def _deserialise_message(fields: dict) -> tuple[PaymentRequest, int]:
    retry_count = int(fields.get("retry_count", "0"))
    metadata = fields.get("metadata")
    return (
        PaymentRequest(
            txn_id=fields["txn_id"],
            merchant_id=fields["merchant_id"],
            amount=Decimal(fields["amount"]),
            currency=fields["currency"],
            payment_method=fields["payment_method"],
            metadata=json.loads(metadata) if metadata else None,
        ),
        retry_count,
    )


async def _process_message(
    message_id: str,
    fields: dict,
    db_pool: asyncpg.Pool,
    redis: aioredis.Redis,
    worker_id: str,
) -> None:
    try:
        payload, retry_count = _deserialise_message(fields)
    except Exception as exc:
        logger.error(
            "worker.deserialise_failed",
            message_id=message_id,
            error=str(exc),
        )
        await redis.xack(settings.queue_stream_name, settings.consumer_group, message_id)
        return

    log = logger.bind(
        txn_id=payload.txn_id,
        merchant_id=payload.merchant_id,
        worker_id=worker_id,
        message_id=message_id,
        retry_count=retry_count,
    )

    log.info("worker.processing_started")

    try:
        result = await process_transaction(
            payload=payload,
            db_pool=db_pool,
            retry_count=retry_count,
        )

        if result.status == TransactionStatus.SUCCESS:
            await store_response(
                txn_id=payload.txn_id,
                response=result,
                redis=redis,
                ttl_seconds=settings.idempotency_ttl_seconds,
            )
            log.info("worker.processing_completed", status=result.status)
        else:
            log.warning(
                "worker.transaction_failed",
                failure_reason=result.failure_reason,
                retry_count=retry_count,
            )
            await handle_failed_transaction(
                payload=payload,
                failure_reason=result.failure_reason or "PROCESSOR_ERROR",
                retry_count=retry_count,
                db_pool=db_pool,
                redis=redis,
            )

        await redis.xack(settings.queue_stream_name, settings.consumer_group, message_id)

    except Exception as exc:
        log.error("worker.unexpected_error", error=str(exc))


async def run_worker(
    worker_id: str,
    db_pool: asyncpg.Pool,
    redis: aioredis.Redis,
) -> None:
    stream = settings.queue_stream_name
    group = settings.consumer_group

    log = logger.bind(worker_id=worker_id)
    log.info("worker.started", stream=stream, group=group)

    while True:
        try:
            results = await redis.xreadgroup(
                groupname=group,
                consumername=worker_id,
                streams={stream: ">"},
                count=10,
                block=2000,
            )

            if not results:
                continue

            for _, messages in results:
                tasks = [
                    _process_message(
                        message_id=msg_id,
                        fields=fields,
                        db_pool=db_pool,
                        redis=redis,
                        worker_id=worker_id,
                    )
                    for msg_id, fields in messages
                ]
                await asyncio.gather(*tasks)

            try:
                length = await redis.xlen(settings.queue_stream_name)
                queue_depth.set(length)
                dlq_length = await redis.xlen(settings.dlq_stream_name)
                dlq_depth.set(dlq_length)
            except Exception:
                pass

        except asyncio.CancelledError:
            log.info("worker.cancelled")
            break
        except Exception as e:
            error_str = str(e)

            if "NOGROUP" in error_str:
                # Consumer group was lost — Redis was flushed or restarted.
                # Recreate the group and continue without requiring an app restart.
                log.warning(
                    "worker.nogroup_detected",
                    worker_id=worker_id,
                    error=error_str,
                )
                try:
                    await ensure_consumer_group(redis)
                    log.info("worker.consumer_group_recreated", worker_id=worker_id)
                except Exception as recreate_err:
                    log.error(
                        "worker.consumer_group_recreate_failed",
                        error=str(recreate_err),
                    )
                    await asyncio.sleep(2)
            else:
                log.error("worker.loop_error", error=error_str)
                await asyncio.sleep(1)


async def start_workers(
    db_pool: asyncpg.Pool,
    redis: aioredis.Redis,
) -> list[asyncio.Task]:
    await ensure_consumer_group(redis)

    tasks: list[asyncio.Task] = []
    for index in range(settings.num_workers):
        worker_id = f"worker-{index}"
        task = asyncio.create_task(
            run_worker(worker_id, db_pool, redis),
            name=worker_id,
        )
        tasks.append(task)
        logger.info("worker.launched", worker_id=worker_id)

    active_workers.set(len(tasks))
    return tasks
