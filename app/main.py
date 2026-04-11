import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.health import router as health_router
from app.api.metrics import router as metrics_router
from app.api.payments import router as payments_router
from app.config import get_settings
from app.database import close_pool, create_pool
from app.observability.logger import get_logger, setup_logging
from app.observability.metrics import active_workers
from app.redis_client import close_redis, create_redis
from app.workers.consumer import start_workers
from app.workers.dlq_consumer import run_dlq_consumer

settings = get_settings()
setup_logging(debug=settings.debug)
logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("app.starting", queue_backend=settings.queue_backend)
    app.state.db_pool = await create_pool()
    app.state.redis = await create_redis()
    app.state.workers = await start_workers(
        db_pool=app.state.db_pool,
        redis=app.state.redis,
    )
    app.state.dlq_task = asyncio.create_task(
        run_dlq_consumer(
            db_pool=app.state.db_pool,
            redis=app.state.redis,
        ),
        name="dlq-consumer",
    )
    logger.info(
        "app.started",
        num_workers=len(app.state.workers),
        dlq_consumer=True,
    )
    try:
        yield
    finally:
        logger.info("app.shutting_down")
        for task in getattr(app.state, "workers", []):
            task.cancel()
        if getattr(app.state, "dlq_task", None):
            app.state.dlq_task.cancel()
        if getattr(app.state, "workers", None):
            await asyncio.gather(*app.state.workers, return_exceptions=True)
        if getattr(app.state, "dlq_task", None):
            await asyncio.gather(app.state.dlq_task, return_exceptions=True)
        await close_pool(app.state.db_pool)
        await close_redis(app.state.redis)
        active_workers.set(0)
        logger.info("app.stopped")


app = FastAPI(
    title="Payment Simulation System",
    description="High-throughput payment processing simulation",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(health_router)
app.include_router(payments_router)
app.include_router(metrics_router)


@app.get("/", include_in_schema=False)
async def root():
    return {
        "service": "payment-simulation",
        "version": "1.0.0",
        "docs": "/docs",
    }
