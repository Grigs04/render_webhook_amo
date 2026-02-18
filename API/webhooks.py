import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, HTTPException
from dotenv import load_dotenv
import logging

# -------------------- LOGGING --------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -------------------- ENV --------------------

load_dotenv()

# -------------------- APP LIFESPAN --------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 Application started")
    yield
    logger.info("🛑 Application stopped")

app = FastAPI(lifespan=lifespan)

# -------------------- HEALTHCHECK --------------------

@app.get("/ping")
async def ping():
    return {"status": "ok"}

# -------------------- AMOCRM WEBHOOK --------------------

@app.post("/amo/create-invoice")
async def create_invoice_from_amo(request: Request):
    """
    Вебхук от amoCRM.
    Сейчас:
    - принимаем запрос
    - логируем payload
    - возвращаем 200 OK

    Позже здесь появится:
    - запрос в amoCRM API
    - вызов API банка
    """


    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    logger.info("📩 Webhook from amoCRM received")
    logger.info(payload)

    # TODO:
    # lead_id = payload["lead"]["id"]
    # дальше логика

    return {
        "status": "ok",
        "message": "Webhook received"
    }
