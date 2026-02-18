import json
import logging
from fastapi import APIRouter, Request, HTTPException

router = APIRouter()

# -------------------- LOGGING --------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -------------------- HEALTHCHECK --------------------

@router.get("/ping")
async def ping():
    return {"status": "ok"}

# -------------------- AMOCRM WEBHOOK --------------------

@router.post("/amo/create-invoice")
async def create_invoice_from_amo(request: Request):
    # try:
    #     payload = await request.json()
    #     print(payload)
    # except Exception:
    #     raise HTTPException(status_code=400, detail="Invalid JSON")
    #
    # logger.info("📩 Webhook from amoCRM received")
    # logger.info(payload)
    body = await request.body()
    headers = dict(request.headers)

    print("HEADERS:", headers)
    print("RAW BODY:", body.decode("utf-8", errors="ignore"))

    return {
        "status": "ok",
        "message": "Webhook received"
    }
