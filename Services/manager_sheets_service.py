import logging

import anyio

from Clients import amocrm  # noqa: F401 — used in update_manager_sheet
from Clients import google_sheets
from Services.sheets_services import _get_contact_value, _get_custom_field, _get_checkbox, _format_date, _as_text

logger = logging.getLogger("manager-sheets")


def _build_manager_row(lead: dict, contact_value: str) -> tuple[str, list[str]]:
    deal_id = str(lead.get("id") or "")
    cf = lead.get("custom_fields_values") or []

    deal_link = (
        f'=HYPERLINK("https://mafiatimeru.amocrm.ru/leads/detail/{deal_id}"; "{deal_id}")'
        if deal_id else ""
    )
    return (
        deal_id,
        [
            deal_link,
            _format_date(_get_custom_field(cf, "Дата")),
            _get_custom_field(cf, "Тариф"),
            _get_custom_field(cf, "Город"),
            _get_custom_field(cf, "Адрес"),
            _as_text(_get_custom_field(cf, "Время начала")),
            _as_text(_get_custom_field(cf, "Количество часов")),
            _as_text(_get_custom_field(cf, "Количество чел.")),
            _get_custom_field(cf, "Формат"),
            _get_custom_field(cf, "Примечание к заказу"),
            _get_custom_field(cf, "Способ оплаты"),
            contact_value,
            _get_custom_field(cf, "Ведущий"),
            _get_checkbox(cf, "У ведущего есть реквизит"),
            _get_checkbox(cf, "Скинул контакт и всю инфу"),
        ],
    )


async def update_manager_sheet(apply_format: bool = False) -> dict[str, int]:
    from Services.sheets_services import STATUSES, EXCLUDED_STATUSES

    deals_by_id: dict[str, dict] = {}
    contact_cache: dict[int, str] = {}

    for status_id in STATUSES:
        deals = await amocrm.get_deals_by_status(status_id)
        for deal in deals:
            deal_id = deal.get("id")
            if deal_id is not None:
                deals_by_id[str(deal_id)] = deal

    excluded_ids: set[str] = set()
    for status_id in EXCLUDED_STATUSES:
        deals = await amocrm.get_deals_by_status(status_id)
        for deal in deals:
            deal_id = deal.get("id")
            if deal_id is not None:
                excluded_ids.add(str(deal_id))

    rows = []
    for deal in deals_by_id.values():
        embedded_contacts = deal.get("embedded_contacts") or []
        contact_value = ""
        if embedded_contacts:
            contact_id = embedded_contacts[0].get("id")
            if contact_id:
                if contact_id not in contact_cache:
                    contact = await amocrm.get_contact(contact_id)
                    contact_cache[contact_id] = _get_contact_value(contact)
                contact_value = contact_cache[contact_id]
        rows.append(_build_manager_row(deal, contact_value))

    result = await anyio.to_thread.run_sync(
        lambda: google_sheets.upsert_manager_deals(rows, apply_format, None)
    )
    await anyio.to_thread.run_sync(google_sheets.refresh_manager_date_colors)
    return result




async def extend_manager_checkboxes_async() -> None:
    sheet_name = google_sheets._get_sheet_name()
    try:
        service = google_sheets._get_service()
        values = await anyio.to_thread.run_sync(
            lambda: service.spreadsheets().values().get(
                spreadsheetId=google_sheets.MANAGERS_SPREADSHEET_ID,
                range=f"'{sheet_name}'!A:A",
            ).execute()
        )
        last_row = len(values.get("values", []))
        await anyio.to_thread.run_sync(lambda: google_sheets.extend_manager_checkboxes(last_row))
    except Exception:
        logger.exception("manager-sheets: failed to extend checkboxes")
