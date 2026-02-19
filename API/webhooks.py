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
    form = await request.form()
    data = dict(form)
    lead_id = data.get("leads[add][0][id]")


    logger.info("📩 Webhook from amoCRM received")
    logger.info(data)

    return {
        "status": "ok",
        "message": "Webhook received"
    }
