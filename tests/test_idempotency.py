import json
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from app.models import FailureReason, PaymentResponse, TransactionStatus
from app.services.idempotency import (
    _deserialise,
    _make_key,
    _serialise,
    get_cached_response,
    is_duplicate,
    store_response,
)


def make_response(
    txn_id: str = "txn_test_001",
    status: str = TransactionStatus.SUCCESS,
) -> PaymentResponse:
    return PaymentResponse(
        txn_id=txn_id,
        status=status,
        merchant_id="merchant_test",
        amount=Decimal("499.99"),
        currency="INR",
        payment_method="UPI",
        failure_reason=None,
        created_at=datetime(2026, 4, 10, 12, 0, 0, tzinfo=timezone.utc),
        processed_at=datetime(2026, 4, 10, 12, 0, 1, tzinfo=timezone.utc),
        message="Payment processed successfully",
    )


@pytest.fixture
def mock_redis():
    return AsyncMock()


def test_make_key_format():
    assert _make_key("txn_001") == "idempotency:txn_001"


def test_make_key_preserves_txn_id():
    txn_id = "txn_merchant_flipkart_20260410_001"
    assert _make_key(txn_id) == f"idempotency:{txn_id}"


def test_serialise_deserialise_roundtrip():
    original = make_response()
    restored = _deserialise(_serialise(original))

    assert restored.txn_id == original.txn_id
    assert restored.status == original.status
    assert restored.amount == original.amount
    assert restored.currency == original.currency
    assert restored.merchant_id == original.merchant_id


def test_serialise_handles_decimal():
    response = make_response()
    data = json.loads(_serialise(response))
    assert isinstance(data["amount"], str)
    assert data["amount"] == "499.99"


def test_serialise_handles_datetime():
    response = make_response()
    data = json.loads(_serialise(response))
    assert isinstance(data["created_at"], str)
    assert "2026-04-10" in data["created_at"]


def test_deserialise_failed_transaction():
    response = PaymentResponse(
        txn_id="txn_failed_001",
        status=TransactionStatus.FAILED,
        merchant_id="merchant_test",
        amount=Decimal("100.00"),
        currency="INR",
        payment_method="CARD",
        failure_reason=FailureReason.INSUFFICIENT_FUNDS,
        created_at=datetime(2026, 4, 10, 12, 0, 0, tzinfo=timezone.utc),
        processed_at=None,
        message="Payment failed: INSUFFICIENT_FUNDS",
    )
    restored = _deserialise(_serialise(response))
    assert restored.failure_reason == FailureReason.INSUFFICIENT_FUNDS
    assert restored.processed_at is None


@pytest.mark.asyncio
async def test_get_cached_response_miss(mock_redis):
    mock_redis.get.return_value = None
    result = await get_cached_response("txn_new_001", mock_redis)
    assert result is None
    mock_redis.get.assert_called_once_with("idempotency:txn_new_001")


@pytest.mark.asyncio
async def test_get_cached_response_hit(mock_redis):
    original = make_response("txn_cached_001")
    mock_redis.get.return_value = _serialise(original)

    result = await get_cached_response("txn_cached_001", mock_redis)

    assert result is not None
    assert result.txn_id == "txn_cached_001"
    assert result.status == TransactionStatus.SUCCESS
    assert result.amount == Decimal("499.99")


@pytest.mark.asyncio
async def test_get_cached_response_redis_down(mock_redis):
    mock_redis.get.side_effect = Exception("Redis connection refused")
    result = await get_cached_response("txn_001", mock_redis)
    assert result is None


@pytest.mark.asyncio
async def test_store_response_calls_set(mock_redis):
    response = make_response("txn_store_001")
    await store_response("txn_store_001", response, mock_redis, ttl_seconds=3600)

    mock_redis.set.assert_called_once()
    call_args = mock_redis.set.call_args
    assert call_args[0][0] == "idempotency:txn_store_001"
    assert call_args[1]["ex"] == 3600


@pytest.mark.asyncio
async def test_store_response_redis_write_fails_silently(mock_redis):
    mock_redis.set.side_effect = Exception("Redis write timeout")
    response = make_response("txn_store_002")

    await store_response("txn_store_002", response, mock_redis)


@pytest.mark.asyncio
async def test_is_duplicate_true(mock_redis):
    mock_redis.exists.return_value = 1
    assert await is_duplicate("txn_dup_001", mock_redis) is True


@pytest.mark.asyncio
async def test_is_duplicate_false(mock_redis):
    mock_redis.exists.return_value = 0
    assert await is_duplicate("txn_new_001", mock_redis) is False


@pytest.mark.asyncio
async def test_is_duplicate_redis_error_returns_false(mock_redis):
    mock_redis.exists.side_effect = Exception("timeout")
    assert await is_duplicate("txn_001", mock_redis) is False


@pytest.mark.asyncio
async def test_store_uses_default_ttl(mock_redis):
    response = make_response()
    await store_response("txn_ttl_001", response, mock_redis)
    call_args = mock_redis.set.call_args
    assert call_args[1]["ex"] == 86400


@pytest.mark.asyncio
async def test_store_uses_custom_ttl(mock_redis):
    response = make_response()
    await store_response("txn_ttl_002", response, mock_redis, ttl_seconds=7200)
    call_args = mock_redis.set.call_args
    assert call_args[1]["ex"] == 7200
