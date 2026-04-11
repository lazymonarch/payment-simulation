# High-Throughput Payment Simulation System

A production-style payment processing simulation: concurrent API traffic, Redis Streams workers, idempotency, retries with exponential backoff, per-merchant rate limiting, DLQ archival, and live Prometheus plus Grafana.

## What this system does

Simulates the core path of a payment gateway: clients call an async FastAPI service, requests are rate-limited and optionally deduplicated, accepted work is published to a Redis Stream, background workers persist state in PostgreSQL and simulate bank outcomes, transient failures are requeued with backoff, and hard failures land in a DLQ stream that is written to Postgres for review.

## Architecture

```
Client → POST /payments
           │
           ├─ Rate limiter (per-merchant, Redis)
           ├─ Idempotency check (Redis cache, configurable TTL)
           └─ Publish to Redis Stream
                      │
               Consumer workers (×N, asyncio)
                      │
               process_transaction()
                      │
               ┌──────┴──────┐
             SUCCESS       FAILED
               │               │
           Postgres       is_retryable?
           + Redis             │
           cache          ┌────┴────┐
                         YES       NO
                          │         │
                       Retry     → DLQ
                    (backoff)
```

## Tech stack

| Layer | Technology |
|-------|------------|
| API | FastAPI + asyncio |
| Queue | Redis Streams |
| Database | PostgreSQL 15 |
| Cache / idempotency | Redis 7 |
| Monitoring | Prometheus + Grafana |
| Logging | structlog (JSON) |
| Containers | Docker Compose |

## Quick start

**Prerequisites:** Docker Desktop, Python 3.11+

```bash
git clone <your-repository-url>
cd payment-simulation
cp .env.example .env
docker compose up --build
```

Services (defaults from `.env.example`):

| Service | URL |
|---------|-----|
| Payment API | http://localhost:8000 |
| API docs | http://localhost:8000/docs |
| Grafana | http://localhost:3000 (admin / admin) |
| Prometheus | http://localhost:9090 |

## Load simulator

Install dependencies locally (or use a virtualenv):

```bash
pip install -r requirements.txt
```

Examples:

```bash
# Makefile shortcut
make simulate

# Or directly
python simulator/simulate.py --count 1000 --concurrency 100

python simulator/simulate.py \
  --count 500 \
  --concurrency 50 \
  --duplicate-rate 0.1 \
  --timeout-rate 0.05 \
  --base-url http://localhost:8000
```

The simulator checks `/health/ready`, then issues concurrent `POST /payments` calls and prints a latency and outcome summary. Use Grafana (**Payment Simulation** dashboard) while a run is in flight to see counters and histograms update after each Prometheus scrape.

**Test merchants:** `merchant_test` has a low rate limit (see `MERCHANT_LIMITS` in `app/services/rate_limiter.py`). `merchant_timeout_test` forces a retryable `BANK_TIMEOUT` for DLQ and retry demos.

## Tests

```bash
pytest tests/ -v
```

## API overview

### `POST /payments`

Submit a payment. Body (JSON) includes `txn_id`, `merchant_id`, `amount`, optional `currency`, `payment_method`, optional `metadata`.

- **202** — Accepted and queued; poll `GET /payments/{txn_id}` for final status.
- **200** — Idempotent replay: same `txn_id` after a successful completion returns the cached result.
- **429** — Rate limited; includes `Retry-After`.
- **503** — Queue publish failed (e.g. Redis unavailable).

### `GET /payments/{txn_id}`

Returns the stored transaction row or **404** if unknown.

### `GET /health`

Liveness: process is up.

### `GET /health/ready`

Readiness: Postgres and Redis reachable.

### `GET /metrics`

Prometheus exposition format.

## Engineering notes

**Idempotency** — Successful outcomes are cached in Redis under `txn_id` (TTL from settings). Postgres enforces `UNIQUE(txn_id)` as a second line of defense.

**Async queue** — The API returns quickly after `XADD`; simulated bank latency runs in workers, not in the request path.

**Retries** — Retryable failure reasons trigger exponential backoff with jitter, then requeue to the same stream until `max_retries` is exceeded, then DLQ.

**Rate limiting** — Per-merchant counters in Redis using atomic `INCR` plus TTL on first touch in a second bucket. On Redis errors the limiter fails open so traffic is not hard-stopped by a cache outage.

**Observability** — Metrics are defined in `app/observability/metrics.py` and scraped from `/metrics`. Grafana provisioning lives under `infra/grafana/`.

## Repository layout

```
app/
  api/             HTTP routes (payments, health, metrics)
  services/        Queue, processor, idempotency, rate limiter, retry engine
  workers/         Stream consumers and DLQ consumer
  observability/   Logging and Prometheus metrics
infra/
  postgres/        init.sql
  prometheus/      scrape config
  grafana/         datasource, dashboard provisioning, dashboard JSON
simulator/         Load test CLI
tests/             Unit and API tests
```
