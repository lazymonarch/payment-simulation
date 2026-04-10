from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.health import router as health_router
from app.config import get_settings
from app.database import close_pool, create_pool
from app.observability.logger import get_logger, setup_logging
from app.redis_client import close_redis, create_redis

settings = get_settings()
setup_logging(debug=settings.debug)
logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("app.starting", queue_backend=settings.queue_backend)
    app.state.db_pool = await create_pool()
    app.state.redis = await create_redis()
    logger.info("app.started")
    try:
        yield
    finally:
        logger.info("app.shutting_down")
        await close_pool(app.state.db_pool)
        await close_redis(app.state.redis)
        logger.info("app.stopped")


app = FastAPI(
    title="Payment Simulation System",
    description="High-throughput payment processing simulation",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(health_router)


@app.get("/", include_in_schema=False)
async def root():
    return {
        "service": "payment-simulation",
        "version": "1.0.0",
        "docs": "/docs",
    }
