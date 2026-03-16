import anyio
from datetime import date
from typing import Any

from Clients import amocrm
from Clients import google_sheets

STATUSES = [75366150, 78036790]


def _get_custom_field(custom_fields: list[dict[str, Any]], name: str) -> str:
    for field in custom_fields:
        if field.get("field_name") != name:
            continue
        values = field.get("values") or []
        if not values:
            return ""
        value = values[0].get("value")
        if value is None:
            return ""
        return str(value)
    return ""


def _format_date(value: str) -> str:
    if not value:
        return ""
    try:
        timestamp = int(value)
    except ValueError:
        return value
    return date.fromtimestamp(timestamp).isoformat()


def _get_contact_value(contact: dict) -> str:
    custom_fields = contact.get("custom_fields_values") or []
    phone = ""
    tg = ""
    for field in custom_fields:
        field_code = field.get("field_code")
        field_name = (field.get("field_name") or "").lower()
        values = field.get("values") or []
        if not values:
            continue
        value = values[0].get("value")
        if value is None:
            continue
        value = str(value)
        if field_code == "PHONE" or "телефон" in field_name:
            phone = value
        if "telegram" in field_name or "телеграм" in field_name or "tg" == field_name:
            tg = value
    return phone or tg


async def update_deals_sheet(apply_format: bool = False) -> dict[str, int]:
    deals_by_id: dict[str, dict] = {}
    contact_cache: dict[int, str] = {}
    user_cache: dict[int, str] = {}

    for status_id in STATUSES:
        deals = await amocrm.get_deals_by_status(status_id)
        for deal in deals:
            deal_id = deal.get("id")
            if deal_id is None:
                continue
            deals_by_id[str(deal_id)] = deal

    rows: list[tuple[str, list[str]]] = []
    for deal in deals_by_id.values():
        custom_fields = deal.get("custom_fields_values") or []

        deal_id = str(deal.get("id") or "")
        deal_link = ""
        if deal_id:
            deal_link = (
                f'=HYPERLINK("https://mafiatimeru.amocrm.ru/leads/detail/{deal_id}"; "{deal_id}")'
            )
        deal_date = _format_date(_get_custom_field(custom_fields, "Дата"))
        price = str(deal.get("price", 0) or 0)
        transfer = _get_custom_field(custom_fields, "Трансфер")
        city = _get_custom_field(custom_fields, "Город")
        tariff = _get_custom_field(custom_fields, "Тариф")
        start_time = _get_custom_field(custom_fields, "Время начала")
        hours = _get_custom_field(custom_fields, "Количество часов")
        note = _get_custom_field(custom_fields, "Примечание к заказу")
        host = _get_custom_field(custom_fields, "Ведущий")
        payment_method = _get_custom_field(custom_fields, "Способ оплаты")
        host_rate = _get_custom_field(custom_fields, "Ставка ведущего")

        contact_value = ""
        embedded_contacts = deal.get("embedded_contacts") or []
        if embedded_contacts:
            contact_id = embedded_contacts[0].get("id")
            if contact_id:
                if contact_id not in contact_cache:
                    contact = await amocrm.get_contact(contact_id)
                    contact_cache[contact_id] = _get_contact_value(contact)
                contact_value = contact_cache[contact_id]

        manager_value = ""
        manager_id = deal.get("responsible_user_id")
        if manager_id:
            if manager_id not in user_cache:
                user_cache[manager_id] = await amocrm.get_user_name(manager_id)
            manager_value = user_cache[manager_id]

        rows.append(
            (
                deal_id,
                [
                    deal_link,
                    deal_date,
                    price,
                    transfer,
                    city,
                    tariff,
                    start_time,
                    hours,
                    note,
                    contact_value,
                    manager_value,
                    host,
                    payment_method,
                    host_rate,
                    '=ЕСЛИ(ИНДЕКС(N:N;СТРОКА())="";"";ИНДЕКС(C:C;СТРОКА())-ИНДЕКС(N:N;СТРОКА()))',
                ],
            )
        )

    current_ids = {deal_id for deal_id, _ in rows if deal_id}
    return await anyio.to_thread.run_sync(
        google_sheets.upsert_deals, rows, apply_format, current_ids
    )
