from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator


class TransactionStatus:
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    DUPLICATE = "DUPLICATE"


class FailureReason:
    INSUFFICIENT_FUNDS = "INSUFFICIENT_FUNDS"
    BANK_TIMEOUT = "BANK_TIMEOUT"
    INVALID_CARD = "INVALID_CARD"
    FRAUD_BLOCK = "FRAUD_BLOCK"
    PROCESSOR_ERROR = "PROCESSOR_ERROR"
    CONNECTION_ERROR = "CONNECTION_ERROR"


class PaymentMethod:
    UPI = "UPI"
    CARD = "CARD"
    NETBANKING = "NETBANKING"
    WALLET = "WALLET"


class PaymentRequest(BaseModel):
    txn_id: str = Field(
        default_factory=lambda: str(uuid4()),
        min_length=1,
        max_length=64,
        description="Unique transaction ID. If not provided, one is generated.",
    )
    merchant_id: str = Field(
        ...,
        min_length=1,
        max_length=64,
        description="Merchant identifier",
    )
    amount: Decimal = Field(
        ...,
        gt=0,
        decimal_places=2,
        description="Payment amount. Must be positive.",
    )
    currency: str = Field(
        default="INR",
        min_length=3,
        max_length=3,
        description="ISO 4217 currency code",
    )
    payment_method: str = Field(
        default=PaymentMethod.UPI,
        description="Payment method",
    )
    metadata: dict[str, Any] | None = Field(
        default=None,
        description="Optional arbitrary metadata",
    )

    @field_validator("currency")
    @classmethod
    def currency_must_be_uppercase(cls, value: str) -> str:
        return value.upper()

    @field_validator("payment_method")
    @classmethod
    def validate_payment_method(cls, value: str) -> str:
        allowed = {
            PaymentMethod.UPI,
            PaymentMethod.CARD,
            PaymentMethod.NETBANKING,
            PaymentMethod.WALLET,
        }
        normalized = value.upper()
        if normalized not in allowed:
            raise ValueError(f"payment_method must be one of {sorted(allowed)}")
        return normalized

    model_config = {
        "json_schema_extra": {
            "example": {
                "txn_id": "txn_merchant1_001",
                "merchant_id": "merchant_flipkart",
                "amount": "499.99",
                "currency": "INR",
                "payment_method": "UPI",
                "metadata": {"order_id": "ORD_12345"},
            }
        }
    }


class PaymentResponse(BaseModel):
    txn_id: str
    status: str
    merchant_id: str
    amount: Decimal
    currency: str
    payment_method: str
    failure_reason: str | None = None
    created_at: datetime
    processed_at: datetime | None = None
    message: str


class PaymentAcceptedResponse(BaseModel):
    txn_id: str
    status: str = TransactionStatus.PENDING
    message: str = "Payment queued for processing"


class ErrorResponse(BaseModel):
    error: str
    detail: str | None = None
