import asyncio
import json
import random
import time
from datetime import datetime, timezone
from typing import Optional

import asyncpg

from app.models import (
    FailureReason,
    PaymentRequest,
    PaymentResponse,
    TransactionStatus,
)
from app.observability.logger import get_logger
from app.observability.metrics import (
    payment_failures_total,
    payment_processed_total,
    payment_processing_duration_seconds,
)

logger = get_logger(__name__)

SIMULATED_FAILURE_RATE = 0.10
SIMULATED_TIMEOUT_RATE = 0.05

RETRYABLE_FAILURES = {
    FailureReason.BANK_TIMEOUT,
    FailureReason.CONNECTION_ERROR,
    FailureReason.PROCESSOR_ERROR,
}

NON_RETRYABLE_FAILURES = {
    FailureReason.INSUFFICIENT_FUNDS,
    FailureReason.INVALID_CARD,
    FailureReason.FRAUD_BLOCK,
}


async def process_transaction(
    payload: PaymentRequest,
    db_pool: asyncpg.Pool,
    retry_count: int = 0,
) -> PaymentResponse:
    log = logger.bind(
        txn_id=payload.txn_id,
        merchant_id=payload.merchant_id,
        amount=str(payload.amount),
        retry_count=retry_count,
    )

    log.info("processor.started")

    start_time = time.monotonic()
    await _upsert_pending(payload, db_pool)
    outcome = await _simulate_bank_call(payload, log)
    response = await _finalise_transaction(payload, outcome, db_pool, log)

    duration = time.monotonic() - start_time
    payment_processing_duration_seconds.labels(status=response.status).observe(duration)
    payment_processed_total.labels(
        status=response.status,
        payment_method=payload.payment_method,
    ).inc()
    if response.status == TransactionStatus.FAILED and response.failure_reason:
        payment_failures_total.labels(
            failure_reason=response.failure_reason,
            retryable=str(is_retryable(response.failure_reason)),
        ).inc()

    return response


async def _upsert_pending(
    payload: PaymentRequest,
    db_pool: asyncpg.Pool,
) -> None:
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO transactions (
                txn_id, merchant_id, amount, currency,
                payment_method, status, metadata
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
            ON CONFLICT (txn_id) DO UPDATE
                SET updated_at = NOW()
            """,
            payload.txn_id,
            payload.merchant_id,
            payload.amount,
            payload.currency,
            payload.payment_method,
            TransactionStatus.PENDING,
            json.dumps(payload.metadata) if payload.metadata is not None else None,
        )


async def _simulate_bank_call(
    payload: PaymentRequest,
    log,
) -> dict[str, Optional[str] | bool]:
    await asyncio.sleep(random.uniform(0.02, 0.12))

    if payload.merchant_id == "merchant_timeout_test":
        log.warning("processor.bank_timeout", forced=True)
        return {"success": False, "failure_reason": FailureReason.BANK_TIMEOUT}

    roll = random.random()

    if roll < SIMULATED_TIMEOUT_RATE:
        log.warning("processor.bank_timeout", roll=round(roll, 3))
        return {"success": False, "failure_reason": FailureReason.BANK_TIMEOUT}

    if roll < SIMULATED_TIMEOUT_RATE + SIMULATED_FAILURE_RATE:
        failure = random.choice(list(NON_RETRYABLE_FAILURES))
        log.warning("processor.bank_rejected", reason=failure, roll=round(roll, 3))
        return {"success": False, "failure_reason": failure}

    log.info("processor.bank_approved", roll=round(roll, 3))
    return {"success": True, "failure_reason": None}


async def _finalise_transaction(
    payload: PaymentRequest,
    outcome: dict[str, Optional[str] | bool],
    db_pool: asyncpg.Pool,
    log,
) -> PaymentResponse:
    now = datetime.now(timezone.utc)
    status = TransactionStatus.SUCCESS if outcome["success"] else TransactionStatus.FAILED
    failure_reason: Optional[str] = outcome["failure_reason"]  # type: ignore[assignment]

    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE transactions
            SET
                status = $1,
                failure_reason = $2,
                processed_at = $3,
                updated_at = NOW()
            WHERE txn_id = $4
            """,
            status,
            failure_reason,
            now,
            payload.txn_id,
        )

    log.info("processor.completed", status=status, failure_reason=failure_reason)

    message = (
        "Payment processed successfully"
        if outcome["success"]
        else f"Payment failed: {failure_reason}"
    )

    return PaymentResponse(
        txn_id=payload.txn_id,
        status=status,
        merchant_id=payload.merchant_id,
        amount=payload.amount,
        currency=payload.currency,
        payment_method=payload.payment_method,
        failure_reason=failure_reason,
        created_at=now,
        processed_at=now if outcome["success"] else None,
        message=message,
    )


def is_retryable(failure_reason: Optional[str]) -> bool:
    return failure_reason in RETRYABLE_FAILURES
