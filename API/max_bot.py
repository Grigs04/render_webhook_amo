import os

from dotenv import load_dotenv
from fastapi import APIRouter, Header, HTTPException
from maxapi import Bot

from Services.max_report_services import build_stats_command_text, build_weekly_report_text

load_dotenv()

router = APIRouter()

MAX_BOT_TOKEN = os.getenv("MAX_BOT_TOKEN", "")
MAX_WEBHOOK_SECRET = os.getenv("MAX_WEBHOOK_SECRET", "")
MAX_REPORT_CHAT_IDS = [
    int(x.strip())
    for x in os.getenv("MAX_REPORT_CHAT_IDS", "").split(",")
    if x.strip().isdigit()
]

bot = Bot(MAX_BOT_TOKEN)


@router.get("/max/ping")
async def max_ping():
    return {"status": "ok"}


def _extract_text(update: dict) -> str:
    message = update.get("message") or {}
    body = message.get("body") or {}
    return str(body.get("text") or "").strip().lower()


def _extract_recipient(update: dict) -> tuple[int | None, int | None]:
    message = update.get("message") or {}
    recipient = message.get("recipient") or {}
    chat_id = recipient.get("chat_id")
    user_id = recipient.get("user_id")
    if chat_id is None and isinstance(recipient.get("chat"), dict):
        chat_id = recipient["chat"].get("chat_id") or recipient["chat"].get("id")
    if user_id is None and isinstance(recipient.get("user"), dict):
        user_id = recipient["user"].get("user_id") or recipient["user"].get("id")
    return (int(chat_id) if chat_id is not None else None, int(user_id) if user_id is not None else None)


@router.post("/max/webhook")
async def max_webhook(update: dict, x_max_bot_api_secret: str | None = Header(default=None)):
    if MAX_WEBHOOK_SECRET and x_max_bot_api_secret != MAX_WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="Invalid webhook secret")

    if update.get("update_type") != "message_created":
        return {"status": "ok"}

    chat_id, user_id = _extract_recipient(update)
    if chat_id is None and user_id is None:
        return {"status": "ok"}
    text = _extract_text(update)

    if text == "/start":
        await bot.send_message(
            text=(
                "Привет! Я бот микростатистики.\n"
                "Команды:\n"
                "/stats — продажи за последнюю неделю."
            ),
            chat_id=chat_id,
            user_id=user_id if chat_id is None else None,
        )
        return {"status": "ok"}

    if text == "/stats":
        stats_text = await build_stats_command_text()
        await bot.send_message(
            text=stats_text,
            chat_id=chat_id,
            user_id=user_id if chat_id is None else None,
        )
        return {"status": "ok"}

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
