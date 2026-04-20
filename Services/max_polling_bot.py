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
MAX_POLLING_ENABLED = os.getenv("MAX_POLLING_ENABLED", "false").lower() in {"1", "true", "yes"}

bot = Bot(MAX_BOT_TOKEN)
dp = Dispatcher()
_polling_task: asyncio.Task | None = None


@dp.message_created(Command("start"))
async def cmd_start(event: MessageCreated):
    await event.message.answer(
        "Привет! Я бот микростатистики.\nКоманда: /stats — продажи за последнюю неделю."
    )


@dp.message_created(Command("stats"))
async def cmd_stats(event: MessageCreated):
    text = await build_stats_command_text()
    await event.message.answer(text)


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
