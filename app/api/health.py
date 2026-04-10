from fastapi import APIRouter, Request, status
from fastapi.responses import JSONResponse

from app.database import check_connection as check_db
from app.observability.logger import get_logger
from app.redis_client import check_connection as check_redis

logger = get_logger(__name__)
router = APIRouter(tags=["health"])


@router.get("/health")
async def liveness():
    return {"status": "alive"}


@router.get("/health/ready")
async def readiness(request: Request):
    db_ok = await check_db(request.app.state.db_pool)
    redis_ok = await check_redis(request.app.state.redis)
    ready = db_ok and redis_ok

    payload = {
        "status": "ready" if ready else "not_ready",
        "postgres": "connected" if db_ok else "unreachable",
        "redis": "connected" if redis_ok else "unreachable",
    }

    logger.info(
        "health.readiness_checked",
        postgres=payload["postgres"],
        redis=payload["redis"],
        ready=ready,
    )

    return JSONResponse(
        content=payload,
        status_code=status.HTTP_200_OK if ready else status.HTTP_503_SERVICE_UNAVAILABLE,
    )
