from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from app.models import FailureReason, PaymentRequest
from app.services.retry_engine import calculate_backoff, handle_failed_transaction


def make_payload(txn_id: str = "txn_retry_001") -> PaymentRequest:
    return PaymentRequest(
        txn_id=txn_id,
        merchant_id="merchant_test",
        amount=Decimal("500.00"),
        currency="INR",
        payment_method="UPI",
    )


@pytest.fixture
def mock_redis():
    return AsyncMock()


@pytest.fixture
def mock_db_pool():
    return AsyncMock()


def test_backoff_increases_with_attempt():
    delay1 = calculate_backoff(attempt=1, base_delay=1.0)
    delay2 = calculate_backoff(attempt=2, base_delay=1.0)
    delay3 = calculate_backoff(attempt=3, base_delay=1.0)
    assert delay2 > delay1
    assert delay3 > delay2


def test_backoff_attempt_1_is_around_2s():
    delay = calculate_backoff(attempt=1, base_delay=1.0)
    assert 2.0 <= delay <= 2.5


def test_backoff_attempt_2_is_around_4s():
    delay = calculate_backoff(attempt=2, base_delay=1.0)
    assert 4.0 <= delay <= 4.5


def test_backoff_attempt_3_is_around_8s():
    delay = calculate_backoff(attempt=3, base_delay=1.0)
    assert 8.0 <= delay <= 8.5


def test_backoff_jitter_varies():
    delays = {calculate_backoff(attempt=1) for _ in range(10)}
    assert len(delays) > 1


@pytest.mark.asyncio
async def test_non_retryable_goes_to_dlq_immediately(mock_redis, mock_db_pool):
    payload = make_payload()
    with patch("app.services.retry_engine._send_to_dlq", new_callable=AsyncMock) as mock_dlq:
        await handle_failed_transaction(
            payload=payload,
            failure_reason=FailureReason.INSUFFICIENT_FUNDS,
            retry_count=0,
            db_pool=mock_db_pool,
            redis=mock_redis,
        )
        mock_dlq.assert_called_once()


@pytest.mark.asyncio
async def test_fraud_block_goes_to_dlq_immediately(mock_redis, mock_db_pool):
    payload = make_payload()
    with patch("app.services.retry_engine._send_to_dlq", new_callable=AsyncMock) as mock_dlq:
        await handle_failed_transaction(
            payload=payload,
            failure_reason=FailureReason.FRAUD_BLOCK,
            retry_count=0,
            db_pool=mock_db_pool,
            redis=mock_redis,
        )
        mock_dlq.assert_called_once()


@pytest.mark.asyncio
async def test_invalid_card_goes_to_dlq_immediately(mock_redis, mock_db_pool):
    payload = make_payload()
    with patch("app.services.retry_engine._send_to_dlq", new_callable=AsyncMock) as mock_dlq:
        await handle_failed_transaction(
            payload=payload,
            failure_reason=FailureReason.INVALID_CARD,
            retry_count=0,
            db_pool=mock_db_pool,
            redis=mock_redis,
        )
        mock_dlq.assert_called_once()


@pytest.mark.asyncio
async def test_bank_timeout_requeues_on_first_attempt(mock_redis, mock_db_pool):
    payload = make_payload()
    with patch("app.services.retry_engine._send_to_dlq", new_callable=AsyncMock) as mock_dlq, \
         patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        await handle_failed_transaction(
            payload=payload,
            failure_reason=FailureReason.BANK_TIMEOUT,
            retry_count=0,
            db_pool=mock_db_pool,
            redis=mock_redis,
        )
        mock_dlq.assert_not_called()
        mock_sleep.assert_called_once()
        mock_redis.xadd.assert_called_once()


@pytest.mark.asyncio
async def test_requeued_message_has_incremented_retry_count(mock_redis, mock_db_pool):
    payload = make_payload()
    with patch("asyncio.sleep", new_callable=AsyncMock):
        await handle_failed_transaction(
            payload=payload,
            failure_reason=FailureReason.BANK_TIMEOUT,
            retry_count=1,
            db_pool=mock_db_pool,
            redis=mock_redis,
        )
    call_args = mock_redis.xadd.call_args
    message_fields = call_args[0][1]
    assert message_fields["retry_count"] == "2"


@pytest.mark.asyncio
async def test_exhausted_retries_go_to_dlq(mock_redis, mock_db_pool):
    payload = make_payload()
    with patch("app.services.retry_engine._send_to_dlq", new_callable=AsyncMock) as mock_dlq, \
         patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        await handle_failed_transaction(
            payload=payload,
            failure_reason=FailureReason.BANK_TIMEOUT,
            retry_count=3,
            db_pool=mock_db_pool,
            redis=mock_redis,
        )
        mock_dlq.assert_called_once()
        mock_sleep.assert_not_called()
        mock_redis.xadd.assert_not_called()


@pytest.mark.asyncio
async def test_connection_error_retries_then_dlq(mock_redis, mock_db_pool):
    payload = make_payload()
    with patch("app.services.retry_engine._send_to_dlq", new_callable=AsyncMock) as mock_dlq, \
         patch("asyncio.sleep", new_callable=AsyncMock):
        await handle_failed_transaction(
            payload=payload,
            failure_reason=FailureReason.CONNECTION_ERROR,
            retry_count=3,
            db_pool=mock_db_pool,
            redis=mock_redis,
        )
        mock_dlq.assert_called_once()


@pytest.mark.asyncio
async def test_dlq_message_contains_correct_fields(mock_redis, mock_db_pool):
    payload = make_payload("txn_dlq_fields_001")
    with patch("asyncio.sleep", new_callable=AsyncMock):
        await handle_failed_transaction(
            payload=payload,
            failure_reason=FailureReason.INSUFFICIENT_FUNDS,
            retry_count=0,
            db_pool=mock_db_pool,
            redis=mock_redis,
        )
    mock_redis.xadd.assert_called_once()
    call_args = mock_redis.xadd.call_args
    stream_name = call_args[0][0]
    fields = call_args[0][1]

    assert "dlq" in stream_name
    assert fields["txn_id"] == "txn_dlq_fields_001"
    assert fields["failure_reason"] == FailureReason.INSUFFICIENT_FUNDS
    assert fields["retry_count"] == "0"
