import structlog
from fastapi import APIRouter, HTTPException, Request, status

from app.models import PaymentRequest, PaymentResponse
from app.services.payment_processor import process_transaction

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/payments", tags=["payments"])


@router.post(
    "",
    response_model=PaymentResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Submit a payment",
    description="Submit a payment for processing synchronously. Phase 5 will make this async.",
)
async def submit_payment(payload: PaymentRequest, request: Request):
    log = logger.bind(
        txn_id=payload.txn_id,
        merchant_id=payload.merchant_id,
        amount=str(payload.amount),
    )

    log.info("api.payment_received")

    try:
        result = await process_transaction(
            payload=payload,
            db_pool=request.app.state.db_pool,
        )
        log.info("api.payment_completed", status=result.status)
        return result
    except Exception as exc:
        log.error("api.payment_error", error=str(exc))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Payment processing failed: {exc}",
        )


@router.get(
    "/{txn_id}",
    response_model=PaymentResponse,
    summary="Get payment status",
    description="Fetch the current status of a transaction by txn_id.",
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
