import json
from fastapi import APIRouter, Request, HTTPException
from Clients.service import runner

router = APIRouter()

@router.get("/ping")
async def ping():
    return {"status": "ok"}

@router.post("/amo/create-invoice")
async def create_invoice_from_amo(request: Request):
    print(request.json())
    # data = await request.json()
    # try:
    #     entity_id = data.get('data', [{}])[0].get('entity_id')
    #     account_id = data.get('account_id')
    # except Exception as e:
    #     raise HTTPException(status_code=400, detail=str(e))
    #
    # await runner(entity_id)
    #
    # return {
    #     "status": "ok",
    #     "message": "Webhook received",
    #     "entity_id": entity_id
    # }

