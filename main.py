from contextlib import asynccontextmanager
from fastapi import FastAPI
from API.webhooks import router as webhook_router
import Clients.amocrm as amocrm_client
import Clients.tochka as tochka_client

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        yield
    finally:
        if not amocrm_client.client.is_closed:
            await amocrm_client.client.aclose()
        if not tochka_client.client.is_closed:
            await tochka_client.client.aclose()

app = FastAPI(lifespan=lifespan)
app.include_router(webhook_router)

