from fastapi import FastAPI

from app.api.routes import ingest
from app.core.config import settings
from app.core.logging_factory import LoggerFactory
from app.core.request_context import RequestLoggingMiddleware
from app.api.routes.index import router as search_router

app = FastAPI(title=settings.APP_NAME)
app.add_middleware(RequestLoggingMiddleware)

app.include_router(ingest.router)

app.include_router(search_router)

logger = LoggerFactory.get_logger(__name__)


@app.get("/")
def read_root():
    return {"message": "Hello, World!"}


@app.get("/ping")
async def ping():
    logger.info("healthcheck ok", extra={"endpoint": "/ping"})
    return {"status": "ok"}


@app.get("/health")
def health():
    return {
        "app": settings.APP_NAME,
        "env": settings.ENV,
        "debug": settings.DEBUG,
    }
