from fastapi import APIRouter
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

router = APIRouter(tags=["observability"])


@router.get(
    "/metrics",
    response_class=Response,
    summary="Prometheus metrics",
    description="Scraped by Prometheus every 15 seconds.",
    include_in_schema=False,
)
async def metrics():
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )
