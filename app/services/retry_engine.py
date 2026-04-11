import asyncio
import json
import random
from datetime import datetime, timezone
from typing import Optional

import asyncpg
import redis.asyncio as aioredis
import structlog

from app.config import get_settings
from app.models import PaymentRequest
from app.services.payment_processor import is_retryable

logger = structlog.get_logger(__name__)
settings = get_settings()


def calculate_backoff(attempt: int, base_delay: float = 1.0) -> float:
    exponential = base_delay * (2**attempt)
    jitter = random.uniform(0, 0.5)
    return round(exponential + jitter, 3)


async def _send_to_dlq(
    payload: PaymentRequest,
    failure_reason: str,
    retry_count: int,
    error_trace: Optional[str],
    redis: aioredis.Redis,
) -> None:
    failed_at = datetime.now(timezone.utc).isoformat()
    dlq_message = {
        "txn_id": payload.txn_id,
        "merchant_id": payload.merchant_id,
        "amount": str(payload.amount),
        "currency": payload.currency,
        "payment_method": payload.payment_method,
        "failure_reason": failure_reason,
        "retry_count": str(retry_count),
        "failed_at": failed_at,
        "all_attempt_times": json.dumps([failed_at]),
        "original_metadata": json.dumps(payload.metadata) if payload.metadata else "",
        "error_trace": error_trace or "",
    }

    await redis.xadd(settings.dlq_stream_name, dlq_message)

    logger.warning(
        "dlq.message_sent",
        txn_id=payload.txn_id,
        merchant_id=payload.merchant_id,
        failure_reason=failure_reason,
        retry_count=retry_count,
    )


async def handle_failed_transaction(
    payload: PaymentRequest,
    failure_reason: str,
    retry_count: int,
    db_pool: asyncpg.Pool,
    redis: aioredis.Redis,
    error_trace: Optional[str] = None,
) -> None:
    log = logger.bind(
        txn_id=payload.txn_id,
        merchant_id=payload.merchant_id,
        failure_reason=failure_reason,
        retry_count=retry_count,
    )

    if not is_retryable(failure_reason):
        log.warning("retry.non_retryable_sending_to_dlq")
        await _send_to_dlq(
            payload=payload,
            failure_reason=failure_reason,
            retry_count=retry_count,
            error_trace=error_trace,
            redis=redis,
        )
        return

    next_attempt = retry_count + 1

    if next_attempt > settings.max_retries:
        log.warning("retry.exhausted_sending_to_dlq", max_retries=settings.max_retries)
        await _send_to_dlq(
            payload=payload,
            failure_reason=failure_reason,
            retry_count=retry_count,
            error_trace=error_trace,
            redis=redis,
        )
        return

    delay = calculate_backoff(
        attempt=next_attempt,
        base_delay=settings.base_retry_delay_seconds,
    )

    log.info(
        "retry.scheduled",
        next_attempt=next_attempt,
        delay_seconds=delay,
        max_retries=settings.max_retries,
    )

    await asyncio.sleep(delay)

    retry_message = payload.model_dump(mode="python")
    retry_message["retry_count"] = str(next_attempt)
    retry_message = {
        key: json.dumps(value) if isinstance(value, (dict, list)) else str(value)
        for key, value in retry_message.items()
        if value is not None
    }

    await redis.xadd(settings.queue_stream_name, retry_message)

    log.info(
        "retry.requeued",
        next_attempt=next_attempt,
        stream=settings.queue_stream_name,
    )
