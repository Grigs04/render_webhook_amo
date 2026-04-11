import logging
from fastapi import APIRouter, Request, HTTPException, BackgroundTasks
from Services.invoise_services import runner, checkker
from Services.act_services import runner as act_runner
from Services.agreement_services import run as agreement_runner
from Services.sheets_services import update_deals_sheet
from Services.sync_services import sync_crm_to_db

router = APIRouter()
logger = logging.getLogger("webhooks")


async def _run_invoice_background(entity_id: int):
    logger.info("create-invoice background start lead_id=%s", entity_id)
    result = await runner(entity_id)
    if result.get("status") != "ok":
        logger.error("create-invoice background failed lead_id=%s detail=%s", entity_id, result.get("detail"))
    else:
        logger.info("create-invoice background completed lead_id=%s", entity_id)

@router.get("/ping")
async def ping():
    return {"status": "ok"}

@router.post("/amo/invoice")
async def create_invoice_from_amo(request: Request, background_tasks: BackgroundTasks):
    logger.info("create-invoice webhook received")
    form = await request.form()

    raw_id = form.get('leads[add][0][id]')
    if raw_id is None:
        logger.warning("create-invoice missing lead id")
        raise HTTPException(status_code=400, detail="Missing lead id")
    entity_id = int(raw_id)

    logger.info("create-invoice lead_id=%s", entity_id)
    background_tasks.add_task(_run_invoice_background, entity_id)
    logger.info("create-invoice accepted lead_id=%s", entity_id)

    return {
        "status": "ok",
        "message": "Webhook received, invoice task queued",
        "entity_id": entity_id
    }

@router.post("/amo/act")
async def create_act_from_amo(request: Request):
    logger.info("create-act webhook received")
    form = await request.form()

    raw_id = form.get('leads[add][0][id]')
    if raw_id is None:
        logger.warning("create-act missing lead id")
        raise HTTPException(status_code=400, detail="Missing lead id")
    entity_id = int(raw_id)

    logger.info("create-act lead_id=%s", entity_id)
    result = await act_runner(entity_id)
    if result.get("status") != "ok":
        logger.error("create-act failed lead_id=%s detail=%s", entity_id, result.get("detail"))
        raise HTTPException(status_code=400, detail=result)
    logger.info("create-act completed lead_id=%s", entity_id)

    return {
        "status": "ok",
        "message": "Webhook received",
        "entity_id": entity_id
    }

@router.post('/amo/invoice-status')
async def check_invoice_status():
    # form = await request.form()
    #
    # entity_id = int(form.get('leads[add][0][id]'))
    await checkker()
    return {"status": "ok"}

@router.post('/amo/agreement')
async def create_agreement(request: Request):
    logger.info("create-agreement webhook received")
    form = await request.form()

    raw_id = form.get('leads[add][0][id]')
    if raw_id is None:
        logger.warning("create-agreement missing lead id")
        raise HTTPException(status_code=400, detail="Missing lead id")
    entity_id = int(raw_id)

    logger.info("create-agreement lead_id=%s", entity_id)
    result = await agreement_runner(entity_id)
    if result.get("status") != "ok":
        logger.error("create-agreement failed lead_id=%s detail=%s", entity_id, result.get("detail"))
        raise HTTPException(status_code=400, detail=result)
    logger.info("create-agreement completed lead_id=%s", entity_id)

    return {
        "status": "ok",
        "message": "Webhook received",
        "entity_id": entity_id
    }

@router.post('/amo/table')
async def update_table(format: int = 0):
    result = await update_deals_sheet(apply_format=bool(format))
    return {"status": "ok", **result}


@router.post("/amo/sync-db")
async def sync_db(updated_from: int | None = None, full: bool = False):
    result = await sync_crm_to_db(updated_from=updated_from, full=full)
    return {"status": "ok", **result}
