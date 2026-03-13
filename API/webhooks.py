from fastapi import APIRouter, Request, HTTPException
from Services.invoise_services import runner, checkker

router = APIRouter()

@router.get("/ping")
async def ping():
    return {"status": "ok"}

@router.post("/amo/create-invoice")
async def create_invoice_from_amo(request: Request):
    form = await request.form()

    raw_id = form.get('leads[add][0][id]')
    if raw_id is None:
        raise HTTPException(status_code=400, detail="Missing lead id")
    entity_id = int(raw_id)


    await runner(entity_id)

    return {
        "status": "ok",
        "message": "Webhook received",
        "entity_id": entity_id
    }

@router.post('/amo/check_invoice_status')
async def check_invoice_status():
    # form = await request.form()
    #
    # entity_id = int(form.get('leads[add][0][id]'))
    await checkker()
    return {"status": "ok"}

@router.post('/amo/create-agreement')
async def create_agreement(request: Request):
    raise HTTPException(status_code=501, detail="Not implemented")
