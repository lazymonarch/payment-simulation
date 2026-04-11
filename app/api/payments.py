import asyncio
import time

import structlog
from fastapi import APIRouter, Request, HTTPException, status
from fastapi.responses import JSONResponse

from app.models import (
    PaymentAcceptedResponse,
    PaymentRequest,
    PaymentResponse,
    TransactionStatus,
)
from app.observability.metrics import (
    api_request_duration_seconds,
    idempotency_hits_total,
    payment_requests_total,
    rate_limit_hits_total,
)
from app.services.idempotency import get_cached_response
from app.services.queue_publisher import publish_payment
from app.services.rate_limiter import check_rate_limit, log_rate_limit_event

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/payments", tags=["payments"])


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit a payment",
    description="Rate limited per merchant. Publishes to queue on success. Poll GET /payments/{txn_id} for result.",
)
async def submit_payment(
    payload: PaymentRequest,
    request: Request,
):
    start = time.monotonic()

    log = logger.bind(
        txn_id=payload.txn_id,
        merchant_id=payload.merchant_id,
        amount=str(payload.amount),
    )

    log.info("api.payment_received")

    # ── Step 1: Rate limit check ──────────────────────────────────────────────
    allowed, current, limit = await check_rate_limit(
        merchant_id=payload.merchant_id,
        redis=request.app.state.redis,
    )

    if not allowed:
        log.warning(
            "api.rate_limited",
            current=current,
            limit=limit,
        )

        rate_limit_hits_total.labels(merchant_id=payload.merchant_id).inc()

        asyncio.create_task(
            log_rate_limit_event(
                merchant_id=payload.merchant_id,
                current_rate=float(current),
                db_pool=request.app.state.db_pool,
            )
        )

        api_request_duration_seconds.labels(
            method="POST",
            endpoint="/payments",
            status_code="429",
        ).observe(time.monotonic() - start)

        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail={
                "error": "Rate limit exceeded",
                "merchant_id": payload.merchant_id,
                "current": current,
                "limit": limit,
                "retry_after_seconds": 1,
            },
            headers={"Retry-After": "1"},
        )

    # ── Step 2: Idempotency check ─────────────────────────────────────────────
    cached = await get_cached_response(
        txn_id=payload.txn_id,
        redis=request.app.state.redis,
    )

    if cached is not None:
        log.info("api.duplicate_request", returning_status=cached.status)

        idempotency_hits_total.labels(merchant_id=payload.merchant_id).inc()
        payment_requests_total.labels(
            merchant_id=payload.merchant_id,
            status="duplicate",
        ).inc()

        api_request_duration_seconds.labels(
            method="POST",
            endpoint="/payments",
            status_code="200",
        ).observe(time.monotonic() - start)

        return JSONResponse(
            content=cached.model_dump(mode="json"),
            status_code=status.HTTP_200_OK,
        )

    # ── Step 3: Publish to queue ──────────────────────────────────────────────
    try:
        message_id = await publish_payment(
            payload=payload,
            redis=request.app.state.redis,
        )
    except Exception as e:
        log.error("api.publish_failed", error=str(e))
        api_request_duration_seconds.labels(
            method="POST",
            endpoint="/payments",
            status_code="503",
        ).observe(time.monotonic() - start)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Queue unavailable. Please retry.",
        )

    payment_requests_total.labels(
        merchant_id=payload.merchant_id,
        status="queued",
    ).inc()

    api_request_duration_seconds.labels(
        method="POST",
        endpoint="/payments",
        status_code="202",
    ).observe(time.monotonic() - start)

    log.info("api.payment_queued", message_id=message_id)

    return PaymentAcceptedResponse(
        txn_id=payload.txn_id,
        status=TransactionStatus.PENDING,
        message="Payment queued for processing",
    )


@router.get(
    "/{txn_id}",
    response_model=PaymentResponse,
    summary="Get payment status",
    description="Returns the current status. Poll after POST until status is SUCCESS or FAILED.",
)
async def get_payment(txn_id: str, request: Request):
    start = time.monotonic()
    log = logger.bind(txn_id=txn_id)

    async with request.app.state.db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                txn_id, merchant_id, amount, currency,
                payment_method, status, failure_reason,
                created_at, processed_at
            FROM transactions
            WHERE txn_id = $1
            """,
            txn_id,
        )

    if not row:
        log.warning("api.payment_not_found")
        api_request_duration_seconds.labels(
            method="GET",
            endpoint="/payments/{txn_id}",
            status_code="404",
        ).observe(time.monotonic() - start)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Transaction {txn_id} not found",
        )

    log.info("api.payment_fetched", status=row["status"])

    api_request_duration_seconds.labels(
        method="GET",
        endpoint="/payments/{txn_id}",
        status_code="200",
    ).observe(time.monotonic() - start)

    return PaymentResponse(
        txn_id=row["txn_id"],
        status=row["status"],
        merchant_id=row["merchant_id"],
        amount=row["amount"],
        currency=row["currency"],
        payment_method=row["payment_method"],
        failure_reason=row["failure_reason"],
        created_at=row["created_at"],
        processed_at=row["processed_at"],
        message=f"Transaction status: {row['status']}",
    )
