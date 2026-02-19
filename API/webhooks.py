import json
import logging
from fastapi import APIRouter, Request, HTTPException

router = APIRouter()

@router.get("/ping")
async def ping():
    return {"status": "ok"}


@router.post("/amo/create-invoice")
async def create_invoice_from_amo(request: Request):
    raw_data = await request.body()
    data = json.loads(raw_data)
    with open('body.json', 'w') as f:
        json.dump(data, f)

    return {
        "status": "ok",
        "message": "Webhook received"
    }

