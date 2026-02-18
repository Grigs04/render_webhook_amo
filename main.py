import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from API.webhooks import router as webhook_router

# -------------------- LOGGING --------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -------------------- LIFESPAN --------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 Application started")
    yield
    logger.info("🛑 Application stopped")

# -------------------- APP --------------------

app = FastAPI(lifespan=lifespan)

app.include_router(webhook_router)
