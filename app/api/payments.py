import structlog
from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import JSONResponse

from app.models import PaymentAcceptedResponse, PaymentRequest, PaymentResponse, TransactionStatus
from app.services.idempotency import get_cached_response
from app.services.queue_publisher import publish_payment

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/payments", tags=["payments"])


@router.post(
    "",
    response_model=PaymentAcceptedResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit a payment",
)
async def submit_payment(payload: PaymentRequest, request: Request):
    log = logger.bind(
        txn_id=payload.txn_id,
        merchant_id=payload.merchant_id,
        amount=str(payload.amount),
    )

    log.info("api.payment_received")

    cached = await get_cached_response(
        txn_id=payload.txn_id,
        redis=request.app.state.redis,
    )

    if cached is not None:
        log.info(
            "api.duplicate_request",
            txn_id=payload.txn_id,
            returning_status=cached.status,
        )
        return JSONResponse(
            content=cached.model_dump(mode="json"),
            status_code=status.HTTP_200_OK,
        )

    try:
        message_id = await publish_payment(
            payload=payload,
            redis=request.app.state.redis,
        )
    except Exception as exc:
        log.error("api.publish_failed", error=str(exc))
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Queue unavailable. Please retry.",
        )

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
)
async def get_payment(txn_id: str, request: Request):
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

    if row is None:
        log.warning("api.payment_not_found")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Transaction {txn_id} not found",
        )

    log.info("api.payment_fetched", status=row["status"])

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
