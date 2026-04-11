"""
Central Prometheus metric definitions for the payment simulation app.

Import from here only — avoids duplicate registrations and import cycles.
"""

from prometheus_client import Counter, Gauge, Histogram

# ── Counters — things that only go up ───────────────────────────────────────

payment_requests_total = Counter(
    name="payment_requests_total",
    documentation="Total payment requests received",
    labelnames=["merchant_id", "status"],
)

payment_processed_total = Counter(
    name="payment_processed_total",
    documentation="Total payments fully processed by the worker",
    labelnames=["status", "payment_method"],
)

payment_failures_total = Counter(
    name="payment_failures_total",
    documentation="Payment failures by reason",
    labelnames=["failure_reason", "retryable"],
)

retry_attempts_total = Counter(
    name="retry_attempts_total",
    documentation="Total retry attempts",
    labelnames=["attempt_number"],
)

dlq_messages_total = Counter(
    name="dlq_messages_total",
    documentation="Total messages sent to the dead letter queue",
    labelnames=["failure_reason"],
)

idempotency_hits_total = Counter(
    name="idempotency_hits_total",
    documentation="Duplicate requests caught by the idempotency layer",
    labelnames=["merchant_id"],
)

rate_limit_hits_total = Counter(
    name="rate_limit_hits_total",
    documentation="Requests rejected by the rate limiter",
    labelnames=["merchant_id"],
)

# ── Histograms — latency distributions ───────────────────────────────────────

payment_processing_duration_seconds = Histogram(
    name="payment_processing_duration_seconds",
    documentation="End-to-end payment processing latency in seconds",
    labelnames=["status"],
    buckets=[0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.3, 0.5, 1.0, 2.0],
)

api_request_duration_seconds = Histogram(
    name="api_request_duration_seconds",
    documentation="API endpoint response latency in seconds",
    labelnames=["method", "endpoint", "status_code"],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)

# ── Gauges — current state ───────────────────────────────────────────────────

queue_depth = Gauge(
    name="queue_depth",
    documentation="Current number of messages in the payment queue (stream length)",
)

dlq_depth = Gauge(
    name="dlq_depth",
    documentation="Current number of messages in the dead letter queue (stream length)",
)

active_workers = Gauge(
    name="active_workers",
    documentation="Number of active consumer worker tasks",
)
