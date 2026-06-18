import asyncio
import gc
import logging
import time

import anyio

from Clients import amocrm
from Clients import google_sheets
from Services.sheets_services import _get_contact_value, _get_custom_field, _get_checkbox, _format_date
from Services.manager_sheets_service import _build_manager_row, extend_manager_checkboxes_async

logger = logging.getLogger("sheets-sync")

CONFIRMED_STATUSES = {75366150, 78036790}
COMPLETED_STATUS = 142
PIPELINE_ID = 9411942
SYNC_INTERVAL = 600  # 10 минут

_last_sync_ts: int | None = None
_sync_task: asyncio.Task | None = None


def _build_row(lead: dict, contact_value: str) -> tuple[str, list[str]]:
    deal_id = str(lead.get("id") or "")
    custom_fields = lead.get("custom_fields_values") or []

    deal_link = (
        f'=HYPERLINK("https://mafiatimeru.amocrm.ru/leads/detail/{deal_id}"; "{deal_id}")'
        if deal_id else ""
    )
    return (
        deal_id,
        [
            deal_link,
            _format_date(_get_custom_field(custom_fields, "Дата")),
            str(lead.get("price", 0) or 0),
            _get_custom_field(custom_fields, "Трансфер"),
            _get_custom_field(custom_fields, "Город"),
            _get_custom_field(custom_fields, "Тариф"),
            _get_custom_field(custom_fields, "Время начала"),
            _get_custom_field(custom_fields, "Количество часов"),
            _get_custom_field(custom_fields, "Количество чел."),
            _get_custom_field(custom_fields, "Примечание к заказу"),
            contact_value,
            _get_custom_field(custom_fields, "Ведущий"),
            _get_custom_field(custom_fields, "Способ оплаты"),
            _get_checkbox(custom_fields, "У ведущего есть реквизит"),
            _get_checkbox(custom_fields, "Скинул контакт и всю инфу"),
            _get_custom_field(custom_fields, "Ставка ведущего"),
            '=ЕСЛИ(ИНДЕКС(P:P;СТРОКА())="";"";ИНДЕКС(C:C;СТРОКА())-ИНДЕКС(P:P;СТРОКА()))',
        ],
    )


async def _fetch_lead_with_contact(lead_id: int) -> tuple[dict, str] | None:
    try:
        lead = await amocrm.get_lead(lead_id)
    except Exception:
        logger.exception("sheets-sync: failed to fetch lead %s", lead_id)
        return None

    contact_value = ""
    contacts = lead.get("_embedded", {}).get("contacts") or []
    if contacts:
        contact_id = contacts[0].get("id")
        if contact_id:
            try:
                contact = await amocrm.get_contact(contact_id)
                contact_value = _get_contact_value(contact)
            except Exception:
                logger.warning("sheets-sync: failed to fetch contact %s", contact_id)

    return lead, contact_value


async def run_incremental_sync(since_override: int | None = None) -> dict:
    global _last_sync_ts

    now = int(time.time())
    since = since_override if since_override is not None else (
        _last_sync_ts if _last_sync_ts is not None else now - 3600
    )

    logger.info("sheets-sync: fetching leads updated since %s", since)
    all_leads = await amocrm.get_leads_updated(updated_from=since)

    pipeline_leads = [l for l in all_leads if l.get("pipeline_id") == PIPELINE_ID]
    logger.info("sheets-sync: %d leads in pipeline since last sync", len(pipeline_leads))

    to_upsert_ids: list[int] = []
    to_mark_red_ids: list[str] = []

    for lead in pipeline_leads:
        status_id = lead.get("status_id")
        lead_id = lead.get("id")
        if not lead_id:
            continue
        if status_id in CONFIRMED_STATUSES:
            to_upsert_ids.append(lead_id)
        elif status_id != COMPLETED_STATUS:
            to_mark_red_ids.append(str(lead_id))

    # Один запрос к amoCRM на сделку — строим строки для обеих таблиц сразу
    main_rows: list[tuple[str, list[str]]] = []
    manager_rows: list[tuple[str, list[str]]] = []

    for lead_id in to_upsert_ids:
        result = await _fetch_lead_with_contact(lead_id)
        if result is None:
            continue
        lead, contact_value = result
        main_rows.append(_build_row(lead, contact_value))
        manager_rows.append(_build_manager_row(lead, contact_value))

    # Запись в основную таблицу
    upserted = 0
    if main_rows:
        res = await anyio.to_thread.run_sync(lambda: google_sheets.upsert_deals(main_rows, False))
        upserted = res.get("updated", 0) + res.get("added", 0)
        logger.info("sheets-sync: main upserted %d", upserted)
        upserted_ids = [did for did, _ in main_rows]
        await anyio.to_thread.run_sync(lambda: google_sheets.reset_deals_color(upserted_ids))

    # Запись в таблицу менеджеров
    mgr_added = 0
    if manager_rows:
        res = await anyio.to_thread.run_sync(lambda: google_sheets.upsert_manager_deals(manager_rows, False))
        mgr_added = res.get("added", 0)
        logger.info("sheets-sync: managers upserted %d", res.get("updated", 0) + mgr_added)
        upserted_ids = [did for did, _ in manager_rows]
        await anyio.to_thread.run_sync(lambda: google_sheets.reset_manager_deals_color(upserted_ids))
        if mgr_added > 0:
            await extend_manager_checkboxes_async()

    # Красный цвет в обеих таблицах
    marked_red = 0
    if to_mark_red_ids:
        marked_red = await anyio.to_thread.run_sync(lambda: google_sheets.mark_deals_red(to_mark_red_ids))
        await anyio.to_thread.run_sync(lambda: google_sheets.mark_manager_deals_red(to_mark_red_ids))
        logger.info("sheets-sync: marked red %d", marked_red)

    if since_override is None:
        _last_sync_ts = now
    return {"upserted": upserted, "marked_red": marked_red, "since": since}


async def _sync_loop():
    logger.info("sheets-sync loop started, interval=%ds", SYNC_INTERVAL)
    while True:
        try:
            await run_incremental_sync()
        except Exception:
            logger.exception("sheets-sync: unhandled error in sync loop")
        gc.collect()
        await asyncio.sleep(SYNC_INTERVAL)


async def start_sheets_sync():
    global _sync_task
    if _sync_task and not _sync_task.done():
        return
    _sync_task = asyncio.create_task(_sync_loop(), name="sheets-sync")
    logger.info("sheets-sync task created")


async def stop_sheets_sync():
    global _sync_task
    if _sync_task and not _sync_task.done():
        _sync_task.cancel()
        try:
            await _sync_task
        except asyncio.CancelledError:
            pass
    _sync_task = None
