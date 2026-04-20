import asyncio
import logging
import os

from dotenv import load_dotenv
from maxapi import Bot, Dispatcher
from maxapi.types import Command, MessageCreated

from Services.max_report_services import build_stats_command_text

load_dotenv()

logger = logging.getLogger("max-polling")

MAX_BOT_TOKEN = os.getenv("MAX_BOT_TOKEN", "")
MAX_POLLING_ENABLED = os.getenv("MAX_POLLING_ENABLED", "true").lower() in {"1", "true", "yes"}
MAX_BOT_PASSWORD = os.getenv("MAX_BOT_PASSWORD", "").strip()

bot = Bot(MAX_BOT_TOKEN)
dp = Dispatcher()
_polling_task: asyncio.Task | None = None
_authorized_chat_ids: set[int] = set()


def _chat_id(event: MessageCreated) -> int | None:
    recipient = getattr(event.message, "recipient", None)
    chat_id = getattr(recipient, "chat_id", None)
    if chat_id is not None:
        return int(chat_id)
    sender = getattr(event.message, "sender", None)
    user_id = getattr(sender, "user_id", None)
    if user_id is not None:
        return int(user_id)
    return None


def _message_text(event: MessageCreated) -> str:
    body = getattr(event.message, "body", None)
    text = getattr(body, "text", None)
    return (text or "").strip()


def _is_authorized(event: MessageCreated) -> bool:
    if not MAX_BOT_PASSWORD:
        return True
    cid = _chat_id(event)
    if cid is None:
        return False
    return cid in _authorized_chat_ids


async def _require_password(event: MessageCreated) -> None:
    await event.message.answer(
        "🔒 Для доступа к боту отправьте пароль одним сообщением."
    )


@dp.message_created(Command("start"))
async def cmd_start(event: MessageCreated):
    if not _is_authorized(event):
        await _require_password(event)
        return
    await event.message.answer(
        "👋 Привет! Я бот микростатистики.\n"
        "📌 Доступные команды:\n"
        "/stats — продажи за последние 7 дней\n"
        "/help — список команд\n"
        "/contacts — контакты"
    )


@dp.message_created(Command("stats"))
async def cmd_stats(event: MessageCreated):
    if not _is_authorized(event):
        await _require_password(event)
        return
    text = await build_stats_command_text()
    await event.message.answer(text)


@dp.message_created(Command("help"))
async def cmd_help(event: MessageCreated):
    if not _is_authorized(event):
        await _require_password(event)
        return
    await event.message.answer(
        "ℹ️ Доступные команды:\n"
        "/start — приветствие\n"
        "/stats — статистика за 7 дней\n"
        "/help — список команд\n"
        "/contacts — контакты"
    )


@dp.message_created(Command("contacts"))
async def cmd_contacts(event: MessageCreated):
    if not _is_authorized(event):
        await _require_password(event)
        return
    await event.message.answer(
        "📞 Контакты:\n"
        "Это заглушка. Добавьте сюда нужные контакты команды."
    )


@dp.message_created()
async def auth_message_handler(event: MessageCreated):
    if _is_authorized(event):
        return

    text = _message_text(event)
    cid = _chat_id(event)
    if not text or cid is None:
        await _require_password(event)
        return

    if text == MAX_BOT_PASSWORD:
        _authorized_chat_ids.add(cid)
        await event.message.answer(
            "✅ Доступ открыт.\n"
            "Используйте /start или /stats."
        )
        return

    await event.message.answer("❌ Неверный пароль. Попробуйте еще раз.")


async def _run_polling():
    # If webhook was configured earlier, polling will not receive updates.
    await bot.delete_webhook()
    logger.info("MAX polling started")
    await dp.start_polling(bot)


async def start_polling_if_enabled():
    global _polling_task
    if not MAX_POLLING_ENABLED:
        logger.info("MAX polling is disabled")
        return
    if not MAX_BOT_TOKEN:
        logger.warning("MAX polling not started: MAX_BOT_TOKEN is empty")
        return
    if _polling_task and not _polling_task.done():
        return
    logger.info("MAX polling is enabled, creating polling task")
    _polling_task = asyncio.create_task(_run_polling(), name="max-polling")


async def stop_polling():
    global _polling_task
    if _polling_task and not _polling_task.done():
        _polling_task.cancel()
        try:
            await _polling_task
        except asyncio.CancelledError:
            pass
    _polling_task = None
