"""
HTTP integration tests for the payment API.

Lifespan (Postgres, Redis, workers) is stubbed so tests run without Docker.
"""

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.models import PaymentResponse, TransactionStatus


@pytest.fixture(scope="module", autouse=True)
def _stub_app_lifespan_dependencies():
    """Allow TestClient to run FastAPI lifespan without real Postgres/Redis."""
    mock_pool = MagicMock()
    mock_redis = AsyncMock()

    async def mock_create_pool():
        return mock_pool

    async def mock_create_redis():
        return mock_redis

    async def mock_start_workers(db_pool, redis):
        async def idle_worker():
            try:
                while True:
                    await asyncio.sleep(3600)
            except asyncio.CancelledError:
                raise

        return [asyncio.create_task(idle_worker()) for _ in range(4)]

    async def mock_run_dlq(db_pool, redis):
        try:
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            raise

    with patch("app.main.create_pool", side_effect=mock_create_pool), patch(
        "app.main.create_redis", side_effect=mock_create_redis
    ), patch("app.main.start_workers", side_effect=mock_start_workers), patch(
        "app.main.run_dlq_consumer", side_effect=mock_run_dlq
    ), patch("app.main.close_pool", new_callable=AsyncMock), patch(
        "app.main.close_redis", new_callable=AsyncMock
    ):
        yield


@pytest.fixture
def client():
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c


@pytest.fixture
def mock_app_state(client):
    """Replace app.state deps after lifespan startup so each test controls mocks."""
    mock_pool = MagicMock()
    mock_redis = AsyncMock()
    app.state.db_pool = mock_pool
    app.state.redis = mock_redis
    return mock_pool, mock_redis


def test_root_returns_service_info(client, mock_app_state):
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert data["service"] == "payment-simulation"
    assert "docs" in data


def test_liveness_always_returns_200(client, mock_app_state):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "alive"}


def test_readiness_returns_200_when_deps_healthy(client, mock_app_state):
    mock_pool, mock_redis = mock_app_state

    mock_conn = AsyncMock()
    mock_conn.fetchval = AsyncMock(return_value=1)
    mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    mock_redis.ping = AsyncMock(return_value=True)

    response = client.get("/health/ready")
    assert response.status_code == 200
    data = response.json()
    assert data["postgres"] == "connected"
    assert data["redis"] == "connected"


def test_readiness_returns_503_when_redis_down(client, mock_app_state):
    mock_pool, mock_redis = mock_app_state

    mock_conn = AsyncMock()
    mock_conn.fetchval = AsyncMock(return_value=1)
    mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    mock_redis.ping = AsyncMock(side_effect=Exception("Redis down"))

    response = client.get("/health/ready")
    assert response.status_code == 503
    assert response.json()["redis"] == "unreachable"


def test_rate_limited_request_returns_429(client, mock_app_state):
    with patch("app.api.payments.check_rate_limit", return_value=(False, 150, 100)), patch(
        "app.api.payments.log_rate_limit_event", new_callable=AsyncMock
    ):
        response = client.post(
            "/payments",
            json={
                "txn_id": "txn_api_rl_001",
                "merchant_id": "merchant_test",
                "amount": "100.00",
                "payment_method": "UPI",
            },
        )

    assert response.status_code == 429
    assert "Retry-After" in response.headers
    assert response.headers["Retry-After"] == "1"
    data = response.json()
    assert data["detail"]["error"] == "Rate limit exceeded"
    assert data["detail"]["current"] == 150
    assert data["detail"]["limit"] == 100


def test_duplicate_request_returns_200_with_cached_response(client, mock_app_state):
    cached = PaymentResponse(
        txn_id="txn_cached_001",
        status=TransactionStatus.SUCCESS,
        merchant_id="merchant_flipkart",
        amount=Decimal("499.99"),
        currency="INR",
        payment_method="UPI",
        failure_reason=None,
        created_at=datetime(2026, 4, 10, 12, 0, 0, tzinfo=timezone.utc),
        processed_at=datetime(2026, 4, 10, 12, 0, 1, tzinfo=timezone.utc),
        message="Payment processed successfully",
    )

    with patch("app.api.payments.check_rate_limit", return_value=(True, 1, 100)), patch(
        "app.api.payments.get_cached_response", new_callable=AsyncMock, return_value=cached
    ):
        response = client.post(
            "/payments",
            json={
                "txn_id": "txn_cached_001",
                "merchant_id": "merchant_flipkart",
                "amount": "499.99",
                "payment_method": "UPI",
            },
        )

    assert response.status_code == 200
    data = response.json()
    assert data["txn_id"] == "txn_cached_001"
    assert data["status"] == TransactionStatus.SUCCESS


def test_valid_payment_returns_202(client, mock_app_state):
    with patch("app.api.payments.check_rate_limit", return_value=(True, 1, 100)), patch(
        "app.api.payments.get_cached_response", new_callable=AsyncMock, return_value=None
    ), patch("app.api.payments.publish_payment", new_callable=AsyncMock, return_value="1712345678-0"):
        response = client.post(
            "/payments",
            json={
                "txn_id": "txn_api_001",
                "merchant_id": "merchant_flipkart",
                "amount": "799.00",
                "payment_method": "UPI",
            },
        )

    assert response.status_code == 202
    data = response.json()
    assert data["txn_id"] == "txn_api_001"
    assert data["status"] == TransactionStatus.PENDING
    assert "queued" in data["message"].lower()


def test_payment_missing_merchant_id_returns_422(client, mock_app_state):
    response = client.post(
        "/payments",
        json={
            "txn_id": "txn_api_bad_001",
            "amount": "100.00",
        },
    )
    assert response.status_code == 422
    errors = response.json()["detail"]
    fields = [e["loc"][-1] for e in errors]
    assert "merchant_id" in fields


def test_payment_negative_amount_returns_422(client, mock_app_state):
    response = client.post(
        "/payments",
        json={
            "txn_id": "txn_api_neg_001",
            "merchant_id": "merchant_test",
            "amount": "-50.00",
            "payment_method": "UPI",
        },
    )
    assert response.status_code == 422


def test_payment_invalid_method_returns_422(client, mock_app_state):
    response = client.post(
        "/payments",
        json={
            "txn_id": "txn_api_method_001",
            "merchant_id": "merchant_test",
            "amount": "100.00",
            "payment_method": "BITCOIN",
        },
    )
    assert response.status_code == 422


def test_queue_unavailable_returns_503(client, mock_app_state):
    with patch("app.api.payments.check_rate_limit", return_value=(True, 1, 100)), patch(
        "app.api.payments.get_cached_response", new_callable=AsyncMock, return_value=None
    ), patch(
        "app.api.payments.publish_payment",
        new_callable=AsyncMock,
        side_effect=Exception("Redis stream unavailable"),
    ):
        response = client.post(
            "/payments",
            json={
                "txn_id": "txn_api_503_001",
                "merchant_id": "merchant_flipkart",
                "amount": "100.00",
                "payment_method": "UPI",
            },
        )

    assert response.status_code == 503


def test_get_payment_returns_404_for_unknown_txn(client, mock_app_state):
    mock_pool, _ = mock_app_state

    mock_conn = AsyncMock()
    mock_conn.fetchrow = AsyncMock(return_value=None)
    mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)

    response = client.get("/payments/txn_does_not_exist")
    assert response.status_code == 404


def test_get_payment_returns_200_for_known_txn(client, mock_app_state):
    mock_pool, _ = mock_app_state

    mock_row = {
        "txn_id": "txn_api_get_001",
        "merchant_id": "merchant_flipkart",
        "amount": Decimal("499.99"),
        "currency": "INR",
        "payment_method": "UPI",
        "status": TransactionStatus.SUCCESS,
        "failure_reason": None,
        "created_at": datetime(2026, 4, 10, 12, 0, 0, tzinfo=timezone.utc),
        "processed_at": datetime(2026, 4, 10, 12, 0, 1, tzinfo=timezone.utc),
    }

    mock_conn = AsyncMock()
    mock_conn.fetchrow = AsyncMock(return_value=mock_row)
    mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)

    response = client.get("/payments/txn_api_get_001")
    assert response.status_code == 200
    data = response.json()
    assert data["txn_id"] == "txn_api_get_001"
    assert data["status"] == TransactionStatus.SUCCESS


def test_metrics_endpoint_returns_prometheus_format(client, mock_app_state):
    response = client.get("/metrics")
    assert response.status_code == 200
    assert "text/plain" in response.headers["content-type"]
    assert "payment_requests_total" in response.text
    assert "payment_processed_total" in response.text
