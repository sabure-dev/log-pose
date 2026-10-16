import logging
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from app.api.router import router
from app.database import engine
from app.logging import correlation_id_ctx, setup_logging

setup_logging()
logger = logging.getLogger("order_service")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Order Service starting up...")
    yield
    logger.info("Order Service shutting down, disposing database engine pool...")
    await engine.dispose()


app = FastAPI(
    title="Order Service",
    lifespan=lifespan,
)


@app.middleware("http")
async def correlation_id_middleware(request: Request, call_next):
    correlation_id = (
        request.headers.get("X-Request-Id")
        or request.headers.get("X-Correlation-Id")
        or str(uuid.uuid4())
    )
    request.state.correlation_id = correlation_id
    token = correlation_id_ctx.set(correlation_id)

    try:
        response = await call_next(request)
        response.headers["X-Request-Id"] = correlation_id
        return response
    finally:
        correlation_id_ctx.reset(token)


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    correlation_id = getattr(request.state, "correlation_id", "unknown")
    logger.error(
        f"Unhandled error processing {request.method} {request.url.path}: {exc}",
        exc_info=True,
    )
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "detail": "Internal server error",
            "request_id": correlation_id,
        },
    )


app.include_router(router)
