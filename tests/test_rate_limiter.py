import pytest
from unittest.mock import AsyncMock, patch

from app.services.rate_limiter import (
    check_rate_limit,
    _make_key,
    _get_limit,
    MERCHANT_LIMITS,
)
from app.config import get_settings

settings = get_settings()


# ── Key construction ──────────────────────────────────────────────────────────

def test_make_key_format():
    key = _make_key("merchant_flipkart", 1712345678)
    assert key == "ratelimit:merchant_flipkart:1712345678"


def test_make_key_uses_merchant_id():
    key1 = _make_key("merchant_a", 1000)
    key2 = _make_key("merchant_b", 1000)
    assert key1 != key2


def test_make_key_uses_window():
    key1 = _make_key("merchant_a", 1000)
    key2 = _make_key("merchant_a", 1001)
    assert key1 != key2


# ── Limit resolution ──────────────────────────────────────────────────────────

def test_get_limit_default():
    limit = _get_limit("merchant_unknown_xyz")
    assert limit == settings.default_rate_limit_per_sec


def test_get_limit_override_test_merchant():
    limit = _get_limit("merchant_test")
    assert limit == MERCHANT_LIMITS["merchant_test"]
    assert limit == 10


def test_get_limit_override_premium_merchant():
    limit = _get_limit("merchant_premium")
    assert limit == MERCHANT_LIMITS["merchant_premium"]
    assert limit == 500


# ── Allow under limit ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_request_allowed_under_limit():
    mock_redis = AsyncMock()
    mock_redis.incr.return_value = 1     # First request this window

    allowed, current, limit = await check_rate_limit("merchant_flipkart", mock_redis)

    assert allowed is True
    assert current == 1
    assert limit == settings.default_rate_limit_per_sec


@pytest.mark.asyncio
async def test_expire_called_on_first_request():
    mock_redis = AsyncMock()
    mock_redis.incr.return_value = 1

    await check_rate_limit("merchant_flipkart", mock_redis)

    mock_redis.expire.assert_called_once()
    call_args = mock_redis.expire.call_args
    assert call_args[0][1] == 2    # TTL = 2 seconds


@pytest.mark.asyncio
async def test_expire_not_called_on_subsequent_requests():
    mock_redis = AsyncMock()
    mock_redis.incr.return_value = 5    # 5th request this window

    await check_rate_limit("merchant_flipkart", mock_redis)

    mock_redis.expire.assert_not_called()


@pytest.mark.asyncio
async def test_request_allowed_at_exact_limit():
    mock_redis = AsyncMock()
    mock_redis.incr.return_value = 10   # Exactly at merchant_test limit

    allowed, current, limit = await check_rate_limit("merchant_test", mock_redis)

    assert allowed is True
    assert current == 10
    assert limit == 10


# ── Reject over limit ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_request_rejected_over_limit():
    mock_redis = AsyncMock()
    mock_redis.incr.return_value = 11   # One over merchant_test limit of 10

    allowed, current, limit = await check_rate_limit("merchant_test", mock_redis)

    assert allowed is False
    assert current == 11
    assert limit == 10


@pytest.mark.asyncio
async def test_request_rejected_well_over_limit():
    mock_redis = AsyncMock()
    mock_redis.incr.return_value = 500

    allowed, current, limit = await check_rate_limit("merchant_test", mock_redis)

    assert allowed is False


# ── Redis failure — fail open ─────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_redis_failure_fails_open():
    """
    If Redis is unavailable, the rate limiter must allow the request through.
    Never block payments because of a cache failure.
    """
    mock_redis = AsyncMock()
    mock_redis.incr.side_effect = Exception("Redis connection refused")

    allowed, current, limit = await check_rate_limit("merchant_test", mock_redis)

    assert allowed is True
    assert current == 0


# ── Merchant isolation ────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_different_merchants_have_independent_counters():
    """
    merchant_a being rate limited must not affect merchant_b.
    Each merchant's counter is stored under a different Redis key.
    """
    mock_redis = AsyncMock()

    # merchant_test is over its limit of 10
    mock_redis.incr.return_value = 50
    allowed_a, _, _ = await check_rate_limit("merchant_test", mock_redis)
    assert allowed_a is False

    # merchant_flipkart is under its limit of 100
    mock_redis.incr.return_value = 5
    allowed_b, _, _ = await check_rate_limit("merchant_flipkart", mock_redis)
    assert allowed_b is True


@pytest.mark.asyncio
async def test_correct_redis_key_used_per_merchant():
    """
    Verify the key written to Redis includes the merchant_id.
    Wrong key = wrong counter = wrong merchant getting rate limited.
    """
    mock_redis = AsyncMock()
    mock_redis.incr.return_value = 1

    with patch("app.services.rate_limiter.time.time", return_value=1712345678.0):
        await check_rate_limit("merchant_flipkart", mock_redis)

    call_args = mock_redis.incr.call_args
    key_used = call_args[0][0]
    assert "merchant_flipkart" in key_used
    assert "1712345678" in key_used


@pytest.mark.asyncio
async def test_different_windows_use_different_keys():
    """
    Requests in different seconds must use different Redis keys.
    """
    mock_redis = AsyncMock()
    mock_redis.incr.return_value = 1

    keys_used = []

    async def capture_incr(key):
        keys_used.append(key)
        return 1

    mock_redis.incr.side_effect = capture_incr

    with patch("app.services.rate_limiter.time.time", return_value=1000.0):
        await check_rate_limit("merchant_a", mock_redis)

    with patch("app.services.rate_limiter.time.time", return_value=1001.0):
        await check_rate_limit("merchant_a", mock_redis)

    assert keys_used[0] != keys_used[1]
    assert "1000" in keys_used[0]
    assert "1001" in keys_used[1]