CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS transactions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    txn_id          VARCHAR(64) UNIQUE NOT NULL,
    merchant_id     VARCHAR(64) NOT NULL,
    amount          NUMERIC(18,2) NOT NULL,
    currency        CHAR(3) NOT NULL DEFAULT 'INR',
    status          VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    payment_method  VARCHAR(32),
    failure_reason  VARCHAR(128),
    retry_count     INT DEFAULT 0,
    idempotency_key VARCHAR(64),
    metadata        JSONB,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    processed_at    TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dlq_transactions (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    txn_id            VARCHAR(64) NOT NULL,
    merchant_id       VARCHAR(64) NOT NULL,
    original_payload  JSONB NOT NULL,
    failure_reason    VARCHAR(256) NOT NULL,
    retry_count       INT NOT NULL DEFAULT 0,
    all_attempt_times TIMESTAMPTZ[],
    error_trace       TEXT,
    created_at        TIMESTAMPTZ DEFAULT NOW(),
    resolved          BOOLEAN DEFAULT FALSE,
    resolved_at       TIMESTAMPTZ,
    resolved_by       VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS rate_limit_events (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    merchant_id  VARCHAR(64) NOT NULL,
    event_at     TIMESTAMPTZ DEFAULT NOW(),
    request_rate NUMERIC(10,2)
);

CREATE INDEX IF NOT EXISTS idx_transactions_txn_id
    ON transactions(txn_id);

CREATE INDEX IF NOT EXISTS idx_transactions_merchant_id
    ON transactions(merchant_id);

CREATE INDEX IF NOT EXISTS idx_transactions_status
    ON transactions(status);

CREATE INDEX IF NOT EXISTS idx_dlq_merchant_id
    ON dlq_transactions(merchant_id);

CREATE INDEX IF NOT EXISTS idx_dlq_resolved
    ON dlq_transactions(resolved);
