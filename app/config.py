from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", case_sensitive=False)

    # Application
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    debug: bool = False

    # PostgreSQL
    postgres_host: str = "postgres"
    postgres_port: int = 5432
    postgres_db: str = "payments"
    postgres_user: str = "payuser"
    postgres_password: str = "paypass"
    database_url: str = "postgresql://payuser:paypass@postgres:5432/payments"

    # Redis
    redis_url: str = "redis://redis:6379/0"
    idempotency_ttl_seconds: int = 86400

    # Queue
    queue_backend: str = "redis"
    queue_stream_name: str = "payment.requested"
    dlq_stream_name: str = "payment.dlq"
    consumer_group: str = "payment-processors"
    num_workers: int = 4

    # Retry
    max_retries: int = 3
    base_retry_delay_seconds: float = 1.0

    # Rate limiting
    default_rate_limit_per_sec: int = 100

    # Kafka (optional)
    kafka_bootstrap_servers: str = "localhost:9092"


@lru_cache
def get_settings() -> Settings:
    return Settings()
