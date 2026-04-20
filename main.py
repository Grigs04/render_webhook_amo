import asyncio
from contextlib import asynccontextmanager
import logging
from fastapi import FastAPI
from API.webhooks import router as webhook_router
from API.dashboard import router as dashboard_router
from API.max_bot import router as max_bot_router
import Clients.amocrm as amocrm_client
import Clients.tochka as tochka_client
from Clients.db import init_pool, close_pool
from Services.max_polling_bot import start_polling_if_enabled, stop_polling

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await init_pool()
        await start_polling_if_enabled()
        yield
    finally:
        await stop_polling()
        await close_pool()
        if not amocrm_client.client.is_closed:
            await amocrm_client.client.aclose()
        if not tochka_client.client.is_closed:
            await tochka_client.client.aclose()

app = FastAPI(lifespan=lifespan)
app.include_router(webhook_router)
app.include_router(dashboard_router)
app.include_router(max_bot_router)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
