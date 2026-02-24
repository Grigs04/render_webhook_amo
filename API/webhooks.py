import json
from fastapi import APIRouter, Request, HTTPException
from Clients.service import runner

router = APIRouter()

@router.get("/ping")
async def ping():
    return {"status": "ok"}

@router.post("/amo/create-invoice")
async def create_invoice_from_amo(request: Request):
    data = await request.form()
    print(data)
    # account = json.loads(data.get('account'))
    # lead = json.loads(data.get('leads'))
    # entity_id = lead.get('add')[0].get('id')


    # await runner(entity_id)
    #
    # return {
    #     "status": "ok",
    #     "message": "Webhook received",
    #     "entity_id": entity_id
    # }

