import os

from dotenv import load_dotenv
from fastapi import APIRouter, HTTPException
from maxapi import Bot

from Services.max_report_services import build_weekly_report_text

load_dotenv()

router = APIRouter()

MAX_BOT_TOKEN = os.getenv("MAX_BOT_TOKEN", "")
MAX_REPORT_CHAT_IDS = [
    int(x.strip())
    for x in os.getenv("MAX_REPORT_CHAT_IDS", "").split(",")
    if x.strip().isdigit()
]

bot = Bot(MAX_BOT_TOKEN)


@router.get("/max/ping")
async def max_ping():
    return {"status": "ok"}


@router.post("/max/cron/weekly")
async def max_weekly_cron(chat_id: int | None = None):
    text = await build_weekly_report_text()
    target_chat_ids = [chat_id] if chat_id is not None else MAX_REPORT_CHAT_IDS
    if not target_chat_ids:
        raise HTTPException(status_code=400, detail="No target chat_id configured")
    for cid in target_chat_ids:
        await bot.send_message(chat_id=cid, text=text)
    return {"status": "ok", "sent": len(target_chat_ids)}
